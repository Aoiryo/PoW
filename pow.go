package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/libp2p/go-libp2p"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

// --- Core Data Structures ---

var difficulty = 1 // Difficulty level for PoW
var ErrOutOfRange = errors.New("block index out of range")

type transaction struct {
	Content   string
	Timestamp time.Time
}

type BlockToHash struct {
	Index      int
	Timestamp  time.Time
	Data       string
	PreHash    string
	Nonce      int
	Difficulty int
}

type Block struct {
	Index     int
	Timestamp time.Time
	Data      string
	PreHash   string
	Nonce     int
	Hash      string
}

type BlockNode struct {
	Block    Block
	Parent   *BlockNode
	Children []*BlockNode
	Height   int
}

type Blockchain struct {
	mu         sync.Mutex
	BlockIndex map[string]*BlockNode
	Tips       map[string]*BlockNode
	Head       *BlockNode
}

type Node struct {
	mu             sync.Mutex
	Host           host.Host
	Blockchain     *Blockchain
	Mempool        []transaction
	PubSub         *pubsub.PubSub
	BlockTopic     *pubsub.Topic
	BlockSub       *pubsub.Subscription       // Added field for the subscription
	Discovery      *drouting.RoutingDiscovery // Added field for discovery
	Ctx            context.Context
	cancel         context.CancelFunc // To allow stopping node-specific goroutines
	DiscoveryTag   string             // Tag used for finding peers
	miningLoopWait sync.WaitGroup     // To wait for mining loop to finish
}

// --- Constants for libp2p ---
const BlockTopicName = "blockchain/blocks" // Using topic name as discovery tag
const HeadersProtocolID = "/blockchain/headers/1.0.0"
const BlocksProtocolID = "/blockchain/blocks/1.0.0"
const ChainInfoProtocolID = "/blockchain/chaininfo/1.0.0"

type MessageType int

const (
	RequestHeaders MessageType = iota
	ResponseHeaders
	RequestBlock
	ResponseBlock
)

// Message structure for recovery protocol
type RecoveryMessage struct {
	Type      MessageType
	Data      []byte // Contains serialized headers or blocks
	StartHash string // For header requests
	EndHash   string // For header requests
	BlockHash string // For block requests
}

// Header is a lightweight version of Block used for chain comparison
type BlockHeader struct {
	Index     int
	Hash      string
	PreHash   string
	Timestamp time.Time
}

// Response containing the peer's tip info
type TipResponseMsg struct {
	HeadHash string
	Height   int
}

// Request blocks starting after a known hash, optionally up to a stop hash
type GetBlocksRequestMsg struct {
	StartHash string // The hash of the latest block the requester knows
	StopHash  string // Optional: Request blocks up to this hash (can be empty)
}

// Response containing the requested blocks
type BlocksResponseMsg struct {
	Blocks []Block // The list of blocks being sent
	More   bool    // Indicates if the sender has more blocks after this batch
}

// --- Client Submission Methods ---
// SubmitContent adds content to the node's mempool
func (n *Node) SubmitContent(content string) error {
	transaction := transaction{
		Content:   content,
		Timestamp: time.Now().Truncate(time.Minute).Round(0),
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Mempool = append(n.Mempool, transaction)

	log.Printf("Node %s added content to mempool: %s\n", n.Host.ID().ShortString(), content)
	return nil
}

func (n *Node) SubmitContentWithTimestamp(content string, timestamp time.Time) error {
	transaction := transaction{
		Content:   content,
		Timestamp: timestamp,
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Mempool = append(n.Mempool, transaction)

	log.Printf("Node %s added content to mempool: %s\n", n.Host.ID().ShortString(), content)
	return nil
}

// --- Blockchain Methods ---
// miningLoop continuously attempts to mine new blocks
// mine whenever mempool is not empty, with a small delay
func (n *Node) miningLoop() {
	defer n.miningLoopWait.Done()
	log.Printf("Node %s started mining loop.", n.Host.ID().ShortString())

	ticker := time.NewTicker(5 * time.Second) // Check every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			mempoolSize := len(n.Mempool)
			n.mu.Unlock()

			if mempoolSize > 0 {
				log.Printf("Node %s attempting to mine a block (mempool size: %d)...\n", n.Host.ID().ShortString(), mempoolSize)
				newBlock, err := n.MineBlock()
				if err != nil {
					if err.Error() != "mempool is empty" && err.Error() != "mining cancelled" {
						log.Printf("Node %s mining error: %v\n", n.Host.ID().ShortString(), err)
					}
				} else if newBlock != nil {

					addErr := n.Blockchain.AddBlock(*newBlock) // Add to own chain first
					if addErr != nil {
						log.Printf("Node %s failed to add own mined block: %v\n", n.Host.ID().ShortString(), addErr)
					} else {
						// Successfully added to own chain, remove from mempool
						n.removeFromMempool(*newBlock)
						// Broadcast the new block to other nodes
						broadcastErr := n.BroadcastBlock(*newBlock) // Broadcast if added successfully
						if broadcastErr != nil {
							log.Printf("Node %s failed to broadcast block: %v\n", n.Host.ID().ShortString(), broadcastErr)
						}
					}
				}
			}
		case <-n.Ctx.Done():
			log.Printf("Node %s stopping mining loop due to context cancellation.\n", n.Host.ID().ShortString())
			return
		}
	}
}

// MineBlock picks a random nonce and increases it to find a valid one
func (n *Node) MineBlock() (*Block, error) {

	targetPrefix := ""
	for i := 0; i < difficulty; i++ {
		targetPrefix += "0"
	}

	startTime := time.Now()
	// set nonce as a random number to avoid collisions
	nonce := rand.Int63n(1000000)
	log.Printf("Node %s started mining...\n", n.Host.ID().ShortString())
	for {
		select {
		case <-n.Ctx.Done():
			log.Printf("Node %s mining cancelled.\n", n.Host.ID().ShortString())
			return nil, fmt.Errorf("mining cancelled")
		default:
			n.mu.Lock()
			transaction := n.Mempool[0]
			n.mu.Unlock()

			block := n.Blockchain.NewBlock(transaction, int(nonce))

			hash := block.CalculateHash()
			if hash[:difficulty] == targetPrefix {
				block.Hash = hash
				duration := time.Since(startTime)
				log.Printf("Node %s found block! Hash: %s..., Nonce: %d, Time: %s\n", n.Host.ID().ShortString(), hash[:8], nonce, duration)

				n.mu.Lock()
				n.Mempool = n.Mempool[1:] // Remove the mined transaction from mempool
				n.mu.Unlock()

				return block, nil
			}
			nonce++
		}
	}

}

// NewBlock creates a new block (called after successful mining)
func (bc *Blockchain) NewBlock(transaction transaction, nonce int) *Block {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	block := &Block{
		Index:     bc.Head.Height + 1,
		Timestamp: transaction.Timestamp,
		Data:      transaction.Content,
		PreHash:   bc.Head.Block.Hash,
		Nonce:     nonce,
	}
	return block
}

// CalculateHash computes the hash of the block
func (b *Block) CalculateHash() string {

	blockToHash := BlockToHash{
		Index:      b.Index,
		Timestamp:  b.Timestamp,
		Data:       b.Data,
		PreHash:    b.PreHash,
		Nonce:      b.Nonce,
		Difficulty: difficulty,
	}

	blockBytes, _ := json.Marshal(blockToHash)
	return fmt.Sprintf("%x", sha256.Sum256(blockBytes))
}

// check the timestamp as well?
// removeFromMempool removes a content from the mempool
func (n *Node) removeFromMempool(block Block) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, c := range n.Mempool {
		if c.Content == block.Data && c.Timestamp == block.Timestamp {
			n.Mempool = append(n.Mempool[:i], n.Mempool[i+1:]...)
			break
		}
	}
}

// if a new block is mined, the transaction is removed from the mempool but might Add block fail
func (bc *Blockchain) AddBlock(block Block) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if block.Hash == "" {
		block.Hash = block.CalculateHash()
	}
	log.Printf("Adding block %s at height %d with data: %s",
		block.Hash[:8], block.Index, block.Data)

	// Special case for Genesis block - add it directly without validation
	if block.Index == 0 {
		log.Printf("Genesis block detected, adding without validation")
		blocknode := &BlockNode{
			Block:    block,
			Parent:   nil,
			Children: []*BlockNode{},
			Height:   0,
		}
		bc.BlockIndex[block.Hash] = blocknode
		bc.Tips[block.Hash] = blocknode
		bc.Head = blocknode
		log.Printf("Genesis block %s successfully added to blockchain", block.Hash[:8])
		return nil
	}

	blocknode := &BlockNode{
		Block:    block,
		Parent:   bc.BlockIndex[block.PreHash],
		Children: []*BlockNode{},
		Height:   block.Index,
	}

	if len(bc.BlockIndex) == 0 {
		log.Printf("Cannot add block %s - blockchain is empty (genesis block should exist)",
			blocknode.Block.Hash[:8])
		return fmt.Errorf("cannot add block to empty chain (genesis block should exist)")
	}

	log.Printf("Validating block %s...", blocknode.Block.Hash[:8])
	err := bc.isValidBlock(blocknode.Block)
	if errors.Is(err, ErrOutOfRange) {
		return ErrOutOfRange
	} else if err != nil {
		log.Printf("Block %s validation failed: %v", blocknode.Block.Hash[:8], err)
		return fmt.Errorf("block %s is invalid: %v", blocknode.Block.Hash, err)
	}
	log.Printf("Block %s passed validation", blocknode.Block.Hash[:8])

	prevhash := blocknode.Block.PreHash
	// if prevhash is not in the chain, it's invalid
	if _, ok := bc.BlockIndex[prevhash]; !ok {
		log.Printf("Block %s parent hash %s not found in blockchain",
			blocknode.Block.Hash[:8], prevhash[:8])
		return fmt.Errorf("block %s is invalid (parent block not found)", blocknode.Block.Hash)
	}
	log.Printf("Block %s parent block %s exists in chain",
		blocknode.Block.Hash[:8], prevhash[:8])

	// if block with same data and timestamp exists, it's a duplicate
	log.Printf("Checking if block %s is a duplicate...", blocknode.Block.Hash[:8])
	if !bc.DeDuplicate(blocknode) {
		log.Printf("Block %s is a duplicate", blocknode.Block.Hash[:8])
		return fmt.Errorf("block %s is a duplicate", blocknode.Block.Hash)
	}
	log.Printf("Block %s is not a duplicate", blocknode.Block.Hash[:8])

	// if prevhash is in the chain and not a duplicate, add it to the chain
	log.Printf("Adding block %s as child of %s",
		blocknode.Block.Hash[:8], prevhash[:8])
	bc.BlockIndex[prevhash].Children = append(bc.BlockIndex[prevhash].Children, blocknode)
	bc.BlockIndex[blocknode.Block.Hash] = blocknode
	blocknode.Parent = bc.BlockIndex[prevhash]

	// if it updated the longest chain, update the head
	if blocknode.Height > bc.Head.Height {
		log.Printf("Block %s at height %d is now the new HEAD (previous height: %d, hash: %s)",
			blocknode.Block.Hash[:8], blocknode.Height, bc.Head.Height, bc.Head.Block.Hash[:8])
		bc.Head = blocknode
	} else {
		log.Printf("Block %s added as a side chain (current head height: %d, hash: %s)",
			blocknode.Block.Hash[:8], bc.Head.Height, bc.Head.Block.Hash[:8])
	}

	// if it parents previously a tip, update the tips
	delete(bc.Tips, prevhash)
	bc.Tips[blocknode.Block.Hash] = blocknode
	log.Printf("Block %s added to tips map, total tips: %d",
		blocknode.Block.Hash[:8], len(bc.Tips))

	log.Printf("Block %s successfully added to blockchain", blocknode.Block.Hash[:8])
	return nil
}

func (bc *Blockchain) ChainPruning() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	dummyHead := bc.Head
	longestblockchaine := make(map[string]*BlockNode)
	for {
		longestblockchaine[dummyHead.Block.Hash] = dummyHead
		if dummyHead.Parent == nil {
			break
		}
		dummyHead = dummyHead.Parent
	}

	longestHeight := bc.Head.Height
	for _, tipNode := range bc.Tips {
		if tipNode.Height < longestHeight-6 {
			dummyTip := tipNode
			for {
				if _, ok := longestblockchaine[dummyTip.Block.Hash]; ok {
					break
				}
				delete(bc.BlockIndex, dummyTip.Block.Hash)
				delete(bc.Tips, dummyTip.Block.Hash)
				dummyTipParent := dummyTip.Parent
				for i, child := range dummyTip.Parent.Children {
					if child.Block.Hash == dummyTip.Block.Hash {
						dummyTip.Parent.Children = append(dummyTip.Parent.Children[:i], dummyTip.Parent.Children[i+1:]...)
						break
					}
				}
				dummyTip = dummyTipParent
			}
		}
	}
}

// isValidBlock validates a single block relative to the current chain state
func (bc *Blockchain) isValidBlock(block Block) error {
	log.Printf("Validating block %s at height %d with data: %s", block.Hash[:8], block.Index, block.Data)
	log.Printf("Block details - Index: %d, PreHash: %s, Nonce: %d",
		block.Index, block.PreHash[:8], block.Nonce)

	targetPrefix := ""
	for i := 0; i < difficulty; i++ {
		targetPrefix += "0"
	}
	calculatedHash := block.CalculateHash()

	// Validate hash matches the calculated hash
	if calculatedHash != block.Hash {
		log.Printf("Validation Error: Hash mismatch for block %s", block.Hash[:8])
		log.Printf("  Provided hash: %s", block.Hash[:16])
		log.Printf("  Calculated hash: %s", calculatedHash[:16])
		return fmt.Errorf("hash mismatch for block")
	}

	// Validate PoW difficulty
	if calculatedHash[:difficulty] != targetPrefix {
		log.Printf("Validation Error: PoW difficulty not met for block %s", block.Hash[:8])
		log.Printf("  Required prefix: %s", targetPrefix)
		log.Printf("  Actual prefix: %s", calculatedHash[:difficulty])
		return fmt.Errorf("PoW difficulty not met for block")
	}

	if block.Index < 0 || block.Index > bc.Head.Height+1 {
		log.Printf("Validation Error: Block index out of range for block %s", block.Hash[:8])
		log.Printf("  Index: %d, Max allowed: %d", block.Index, bc.Head.Height+1)
		return ErrOutOfRange
	}

	// check prevhash
	if block.PreHash == "" {
		log.Printf("Validation Error: Genesis block cannot have a parent")
		return ErrOutOfRange
	}
	if _, ok := bc.BlockIndex[block.PreHash]; !ok {
		if block.Index == bc.Head.Height+1 {
			log.Printf("Validation Error: Block index is height + 1 but parents not found %s", block.Hash[:8])
			return ErrOutOfRange
		}
		log.Printf("Validation Error: Parent block %s not found in blockchain", block.PreHash[:8])
		return ErrOutOfRange
	}

	log.Printf("Block %s successfully validated", block.Hash[:8])
	// and more...
	return nil
}

func (bc *Blockchain) DeDuplicate(blocknode *BlockNode) bool {
	// check if the block is already in the chain
	block := blocknode.Block
	dummyBlockNode := blocknode
	for {
		if dummyBlockNode.Parent == nil {
			return true
		}
		dummyBlockNode = dummyBlockNode.Parent
		historyblock := dummyBlockNode.Block
		if block.Data == historyblock.Data && block.Timestamp == historyblock.Timestamp {
			return false
		}
	}
}

// BroadcastBlock uses Libp2p PubSub to broadcast a newly mined block.
func (n *Node) BroadcastBlock(block Block) error {
	log.Printf("Node %s broadcasting block %s via PubSub...\n", n.Host.ID().ShortString(), block.Hash[:8])
	blockBytes, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("failed to marshal block for broadcast: %w", err)
	}

	err = n.BlockTopic.Publish(n.Ctx, blockBytes)
	if err != nil {
		return fmt.Errorf("failed to publish block to pubsub topic: %w", err)
	}
	log.Printf("Node %s: Successfully published block %s to topic %s\n", n.Host.ID().ShortString(), block.Hash[:8], n.BlockTopic.String())
	return nil
}

// --- Node Methods ---

// this function creates and initializes a new Node instance.
func NewNode(ctx context.Context, listenPort int, discoveryTag string) (*Node, error) {
	nodeCtx, nodeCancel := context.WithCancel(ctx)

	// 1. create libp2p Host
	h, err := makeHost(nodeCtx, listenPort)
	if err != nil {
		nodeCancel()
		return nil, fmt.Errorf("failed to make host: %w", err)
	}

	// 2. create Genesis Block (all nodes start with the same genesis)
	genesisBlock := Block{
		Index:     0,
		Timestamp: time.Date(2001, 1, 2, 23, 41, 11, 0, time.UTC),
		Data:      "Genesis Block",
		PreHash:   "",
		Nonce:     0,
	}
	genesisBlock.Hash = genesisBlock.CalculateHash()
	genesisBlockBlockNode := &BlockNode{
		Block:    genesisBlock,
		Parent:   nil,
		Children: []*BlockNode{},
		Height:   0,
	}

	// 3. initialize Blockchain
	blockchain := &Blockchain{
		BlockIndex: make(map[string]*BlockNode),
		Tips:       make(map[string]*BlockNode),
		Head:       genesisBlockBlockNode,
	}
	blockchain.BlockIndex[genesisBlock.Hash] = genesisBlockBlockNode
	blockchain.Tips[genesisBlock.Hash] = genesisBlockBlockNode

	// 4. setup PubSub
	ps, err := pubsub.NewGossipSub(nodeCtx, h)
	if err != nil {
		h.Close()
		nodeCancel()
		return nil, fmt.Errorf("failed to create PubSub: %w", err)
	}

	// 5. join the block topic
	blockTopic, err := ps.Join(BlockTopicName)
	if err != nil {
		h.Close()
		nodeCancel()
		return nil, fmt.Errorf("failed to join block topic: %w", err)
	}

	// 6. subscribe to the block topic
	sub, err := blockTopic.Subscribe(pubsub.WithBufferSize(40960))
	if err != nil {
		blockTopic.Close()
		h.Close()
		nodeCancel()
		return nil, fmt.Errorf("failed to subscribe to block topic: %w", err)
	}

	// 7. setup discovery using Kademlia DHT (may not be necessary for local-only tests)
	discovery, err := setupDiscovery(nodeCtx, h)
	if err != nil {
		sub.Cancel()
		blockTopic.Close()
		h.Close()
		nodeCancel()
		return nil, fmt.Errorf("failed to setup discovery: %w", err)
	}

	node := &Node{
		Host:         h,
		Blockchain:   blockchain,
		PubSub:       ps,
		BlockTopic:   blockTopic,
		BlockSub:     sub,
		Discovery:    discovery,
		Ctx:          nodeCtx,
		cancel:       nodeCancel,
		DiscoveryTag: discoveryTag,
	}

	// 8. set stream handler (not implemented yet)
	h.SetStreamHandler(HeadersProtocolID, node.handleHeadersStream)
	h.SetStreamHandler(BlocksProtocolID, node.handleBlocksStream)
	h.SetStreamHandler(ChainInfoProtocolID, node.handleChainInfoStream)

	return node, nil
}

// findLongestChainFromPeers queries connected peers to find the longest blockchain
func (n *Node) findLongestChainFromPeers() (peer.ID, int, string, error) {
	var longestChainPeer peer.ID
	var maxHeight int = 0
	var tipHash string = ""

	// get list of connected peers
	peers := n.Host.Network().Peers()
	if len(peers) == 0 {
		return longestChainPeer, 0, "", fmt.Errorf("no peers connected")
	}

	// query each peer for their current blockchain height and tip
	for _, peerId := range peers {
		// skip self
		if peerId == n.Host.ID() {
			continue
		}

		height, hash, err := n.requestChainInfo(peerId)
		if err != nil {
			log.Printf("Node %s: Failed to get chain info from peer %s: %v",
				n.Host.ID().ShortString(), peerId.ShortString(), err)
			continue
		}

		if height > maxHeight {
			maxHeight = height
			longestChainPeer = peerId
			tipHash = hash
		}
	}

	if longestChainPeer == "" {
		return longestChainPeer, 0, "", fmt.Errorf("failed to find any valid chains from peers")
	}

	log.Printf("Node %s: Found longest chain from peer %s with height %d and tip hash %s",
		n.Host.ID().ShortString(), longestChainPeer.ShortString(), maxHeight, tipHash)

	return longestChainPeer, maxHeight, tipHash, nil
}

// findCommonAncestor finds the common ancestor between this node's chain and the peer's chain
func (n *Node) findCommonAncestor(peerId peer.ID, peerTipHash string) (string, error) {
	log.Printf("Node %s: Starting search for common ancestor with peer %s from their tip %s",
		n.Host.ID().ShortString(), peerId.ShortString(), peerTipHash[:8])

	// binary search approach to find common ancestor efficiently
	// start with a chunk of headers from the peer's tip going backwards
	log.Printf("Node %s: Requesting initial batch of headers from peer %s",
		n.Host.ID().ShortString(), peerId.ShortString())

	headers, err := n.requestHeaders(peerId, peerTipHash, "", 100)
	if err != nil {
		return "", fmt.Errorf("failed to request headers: %w", err)
	}

	log.Printf("Node %s: Received %d headers from peer %s",
		n.Host.ID().ShortString(), len(headers), peerId.ShortString())

	// Print some info about the received headers
	if len(headers) > 0 {
		log.Printf("Node %s: Headers range from height %d (hash: %s) to height %d (hash: %s)",
			n.Host.ID().ShortString(),
			headers[0].Index, headers[0].Hash[:8],
			headers[len(headers)-1].Index, headers[len(headers)-1].Hash[:8])
	}

	// check if any of these headers are in our blockchain
	for i, header := range headers {
		log.Printf("Node %s: Checking if header %d/%d (hash: %s, height: %d) exists in our chain",
			n.Host.ID().ShortString(), i+1, len(headers), header.Hash[:8], header.Index)

		n.Blockchain.mu.Lock()
		_, exists := n.Blockchain.BlockIndex[header.Hash]
		n.Blockchain.mu.Unlock()

		if exists {
			log.Printf("Node %s: Found common ancestor at block %s (height: %d)",
				n.Host.ID().ShortString(), header.Hash[:8], header.Index)
			return header.Hash, nil
		}

		if header.PreHash == "" {
			// this is the genesis block - always a valid common ancestor
			log.Printf("Node %s: Genesis block found as common ancestor", n.Host.ID().ShortString())
			return header.Hash, nil
		}
	}

	// if not found in the first batch, continue with the oldest header from previous batch
	if len(headers) > 0 {
		oldestHeader := headers[len(headers)-1]
		log.Printf("Node %s: No common ancestor in first batch, continuing search from block %s (height: %d)",
			n.Host.ID().ShortString(), oldestHeader.PreHash[:8], oldestHeader.Index-1)
		return n.recursiveFindCommonAncestor(peerId, oldestHeader.PreHash)
	}

	// if we get here, we couldn't find a common ancestor even at genesis
	log.Printf("Node %s: Failed to find any common ancestor with peer %s",
		n.Host.ID().ShortString(), peerId.ShortString())
	return "", fmt.Errorf("no common ancestor found, chains may be incompatible")
}

// recursiveFindCommonAncestor is a helper for findCommonAncestor that recursively searches backwards
func (n *Node) recursiveFindCommonAncestor(peerId peer.ID, startHash string) (string, error) {
	log.Printf("Node %s: Recursively searching for common ancestor from hash %s",
		n.Host.ID().ShortString(), startHash[:8])

	headers, err := n.requestHeaders(peerId, startHash, "", 100)
	if err != nil {
		return "", fmt.Errorf("failed to request headers: %w", err)
	}

	log.Printf("Node %s: Received %d more headers in recursive search",
		n.Host.ID().ShortString(), len(headers))

	// Print some info about the received headers if we got any
	if len(headers) > 0 {
		log.Printf("Node %s: Headers range from height %d (hash: %s) to height %d (hash: %s)",
			n.Host.ID().ShortString(),
			headers[0].Index, headers[0].Hash[:8],
			headers[len(headers)-1].Index, headers[len(headers)-1].Hash[:8])
	}

	// check if any of these headers are in our blockchain
	for i, header := range headers {
		log.Printf("Node %s: Checking if header %d/%d (hash: %s, height: %d) exists in our chain",
			n.Host.ID().ShortString(), i+1, len(headers), header.Hash[:8], header.Index)

		n.Blockchain.mu.Lock()
		_, exists := n.Blockchain.BlockIndex[header.Hash]
		n.Blockchain.mu.Unlock()

		if exists {
			log.Printf("Node %s: Found common ancestor at block %s (height: %d) during recursive search",
				n.Host.ID().ShortString(), header.Hash[:8], header.Index)
			return header.Hash, nil
		}
	}

	// if we reach genesis block without finding common ancestor
	if len(headers) > 0 && headers[len(headers)-1].PreHash == "" {
		log.Printf("Node %s: Reached genesis without finding common ancestor, chains may be incompatible",
			n.Host.ID().ShortString())
		return "", fmt.Errorf("reached genesis without finding common ancestor")
	}

	// continue with the oldest header from this batch
	if len(headers) > 0 {
		oldestHeader := headers[len(headers)-1]
		log.Printf("Node %s: Continuing recursive search from block %s (height: %d)",
			n.Host.ID().ShortString(), oldestHeader.PreHash[:8], oldestHeader.Index-1)
		return n.recursiveFindCommonAncestor(peerId, oldestHeader.PreHash)
	}

	log.Printf("Node %s: No more headers received in recursive search",
		n.Host.ID().ShortString())
	return "", fmt.Errorf("no more headers received")
}

// requestHeaders requests a batch of headers from a peer
func (n *Node) requestHeaders(peerId peer.ID, startHash, endHash string, limit int) ([]BlockHeader, error) {
	ctx, cancel := context.WithTimeout(n.Ctx, 50*time.Second)
	defer cancel()

	// open stream to peer
	stream, err := n.Host.NewStream(ctx, peerId, HeadersProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream to peer: %w", err)
	}
	defer stream.Close()

	// prepare request
	request := RecoveryMessage{
		Type:      RequestHeaders,
		StartHash: startHash,
		EndHash:   endHash,
		Data:      []byte(fmt.Sprintf("%d", limit)),
	}

	requestBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// send request
	if _, err = stream.Write(requestBytes); err != nil {
		return nil, fmt.Errorf("failed to write to stream: %w", err)
	}

	// read response
	buf := make([]byte, 65536) // 64KB buffer
	bytesRead, err := stream.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read from stream: %w", err)
	}

	// unmarshal response
	var response RecoveryMessage
	if err := json.Unmarshal(buf[:bytesRead], &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if response.Type != ResponseHeaders {
		return nil, fmt.Errorf("unexpected response type: %d", response.Type)
	}

	// unmarshal headers from response data
	var headers []BlockHeader
	if err := json.Unmarshal(response.Data, &headers); err != nil {
		return nil, fmt.Errorf("failed to unmarshal headers: %w", err)
	}

	return headers, nil
}

// syncMissingBlocks downloads and processes missing blocks from the common ancestor to the tip
func (n *Node) syncMissingBlocks(peerId peer.ID, commonAncestorHash, tipHash string) error {
	// get headers from common ancestor to tip to know which blocks to fetch
	headers, err := n.requestHeaders(peerId, tipHash, commonAncestorHash, 1000)
	if err != nil {
		return fmt.Errorf("failed to request headers for sync: %w", err)
	}

	log.Printf("Node %s: Retrieved %d headers to sync from peer %s",
		n.Host.ID().ShortString(), len(headers), peerId.ShortString())

	// Display all the headers we need to sync
	for i, header := range headers {
		log.Printf("Node %s: [%d/%d] Need to sync block: %s (height: %d)",
			n.Host.ID().ShortString(), i+1, len(headers), header.Hash[:8], header.Index)
	}

	// process headers from oldest to newest (reverse the order)
	for i := len(headers) - 1; i >= 0; i-- {
		header := headers[i]

		// skip if we already have this block
		n.Blockchain.mu.Lock()
		_, exists := n.Blockchain.BlockIndex[header.Hash]
		n.Blockchain.mu.Unlock()

		if exists {
			log.Printf("Node %s: Already have block %s, skipping",
				n.Host.ID().ShortString(), header.Hash[:8])
			continue
		}

		log.Printf("Node %s: Requesting block %s at height %d from peer %s",
			n.Host.ID().ShortString(), header.Hash[:8], header.Index, peerId.ShortString())

		// request and process the block
		block, err := n.requestBlock(peerId, header.Hash)
		if err != nil {
			return fmt.Errorf("failed to request block %s: %w", header.Hash[:8], err)
		}

		log.Printf("Node %s: Received block %s, attempting to add to chain",
			n.Host.ID().ShortString(), block.Hash[:8])

		// validate and add the block to our chain
		if err := n.Blockchain.AddBlock(*block); err != nil {
			return fmt.Errorf("failed to add block %s to chain: %w", header.Hash[:8], err)
		}

		log.Printf("Node %s: Successfully recovered block %s at height %d",
			n.Host.ID().ShortString(), block.Hash[:8], block.Index)
	}

	log.Printf("Node %s: Completed syncing all blocks from peer %s",
		n.Host.ID().ShortString(), peerId.ShortString())

	return nil
}

// requestBlock requests a single block from a peer
func (n *Node) requestBlock(peerId peer.ID, blockHash string) (*Block, error) {
	ctx, cancel := context.WithTimeout(n.Ctx, 50*time.Second)
	defer cancel()

	// open stream to peer
	stream, err := n.Host.NewStream(ctx, peerId, BlocksProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream to peer: %w", err)
	}
	defer stream.Close()

	// prepare request
	request := RecoveryMessage{
		Type:      RequestBlock,
		BlockHash: blockHash,
	}

	requestBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// send request
	if _, err = stream.Write(requestBytes); err != nil {
		return nil, fmt.Errorf("failed to write to stream: %w", err)
	}

	// read response
	buf := make([]byte, 65536) // 64KB buffer
	bytesRead, err := stream.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read from stream: %w", err)
	}

	// unmarshal response
	var response RecoveryMessage
	if err := json.Unmarshal(buf[:bytesRead], &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if response.Type != ResponseBlock {
		return nil, fmt.Errorf("unexpected response type: %d", response.Type)
	}

	// unmarshal block from response data
	var block Block
	if err := json.Unmarshal(response.Data, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %w", err)
	}

	return &block, nil
}

// StartRecovery initiates the recovery process when a node rejoins the network
func (n *Node) StartRecovery() {
	log.Printf("Node %s starting recovery process...", n.Host.ID().ShortString())

	// 1. connect to peers and find the longest chain
	longestChainPeer, tipHeight, tipHash, err := n.findLongestChainFromPeers()
	if err != nil {
		log.Printf("Node %s: Failed to find longest chain: %v", n.Host.ID().ShortString(), err)
		return
	}

	log.Printf("Node %s: Found longest chain with height %d and tip %s from peer %s",
		n.Host.ID().ShortString(), tipHeight, tipHash[:8], longestChainPeer.ShortString())

	// 2. find common ancestor
	commonAncestor, err := n.findCommonAncestor(longestChainPeer, tipHash)
	if err != nil {
		log.Printf("Node %s: Failed to find common ancestor: %v", n.Host.ID().ShortString(), err)
		return
	}

	log.Printf("Node %s: Found common ancestor at block %s",
		n.Host.ID().ShortString(), commonAncestor[:8])

	// 3. request and process missing blocks
	err = n.syncMissingBlocks(longestChainPeer, commonAncestor, tipHash)
	if err != nil {
		log.Printf("Node %s: Failed to sync missing blocks: %v", n.Host.ID().ShortString(), err)
		return
	}

	log.Printf("Node %s: Recovery completed successfully", n.Host.ID().ShortString())
}

// handleChainInfoStream handles requests for chain height and tip hash
func (n *Node) handleChainInfoStream(stream network.Stream) {
	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Node %s: Received chain info request from %s",
		n.Host.ID().ShortString(), remotePeer.ShortString())

	// read request (empty in this case, just need to consume it)
	// buf := make([]byte, 128)
	// if _, err := stream.Read(buf); err != nil && err != io.EOF {
	// 	log.Printf("Node %s: Error reading chain info request: %v", n.Host.ID().ShortString(), err)
	// 	stream.Reset()
	// 	return
	// }

	// prepare response with our current height and tip hash
	n.Blockchain.mu.Lock()
	response := struct {
		Height  int
		TipHash string
	}{
		Height:  n.Blockchain.Head.Height,
		TipHash: n.Blockchain.Head.Block.Hash,
	}
	n.Blockchain.mu.Unlock()

	responseBytes, err := json.Marshal(response)
	if err != nil {
		log.Printf("Node %s: Failed to marshal chain info response: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	// send response
	if _, err := stream.Write(responseBytes); err != nil {
		log.Printf("Node %s: Failed to write chain info response: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	stream.Close()
}

// handleHeadersStream handles requests for block headers
func (n *Node) handleHeadersStream(stream network.Stream) {
	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Node %s: Received headers request from %s",
		n.Host.ID().ShortString(), remotePeer.ShortString())

	// read request
	buf := make([]byte, 4096)
	bytesRead, err := stream.Read(buf)
	if err != nil && err != io.EOF {
		log.Printf("Node %s: Error reading headers request: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	// unmarshal request
	var request RecoveryMessage
	if err := json.Unmarshal(buf[:bytesRead], &request); err != nil {
		log.Printf("Node %s: Failed to unmarshal headers request: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	if request.Type != RequestHeaders {
		log.Printf("Node %s: Unexpected message type in headers request: %d",
			n.Host.ID().ShortString(), request.Type)
		stream.Reset()
		return
	}

	// parse limit
	limit := 100 // default
	if len(request.Data) > 0 {
		parsedLimit, err := strconv.Atoi(string(request.Data))
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	// generate headers
	headers := n.generateHeaders(request.StartHash, request.EndHash, limit)

	// marshal headers
	headersBytes, err := json.Marshal(headers)
	if err != nil {
		log.Printf("Node %s: Failed to marshal headers: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	// prepare response
	response := RecoveryMessage{
		Type: ResponseHeaders,
		Data: headersBytes,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		log.Printf("Node %s: Failed to marshal headers response: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	// send response
	if _, err := stream.Write(responseBytes); err != nil {
		log.Printf("Node %s: Failed to write headers response: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	stream.Close()
}

// generateHeaders collects block headers from startHash backwards until endHash (or limit)
func (n *Node) generateHeaders(startHash, endHash string, limit int) []BlockHeader {
	n.Blockchain.mu.Lock()
	defer n.Blockchain.mu.Unlock()

	var headers []BlockHeader

	// if startHash is empty, use the current head
	currentHash := startHash
	if currentHash == "" {
		currentHash = n.Blockchain.Head.Block.Hash
	}

	count := 0
	for currentHash != "" && count < limit {
		blockNode, exists := n.Blockchain.BlockIndex[currentHash]
		if !exists {
			break
		}

		header := BlockHeader{
			Index:     blockNode.Block.Index,
			Hash:      blockNode.Block.Hash,
			PreHash:   blockNode.Block.PreHash,
			Timestamp: blockNode.Block.Timestamp,
		}

		headers = append(headers, header)
		count++

		// Stop if we've reached the endHash
		if currentHash == endHash {
			break
		}

		currentHash = blockNode.Block.PreHash
	}

	return headers
}

// handleBlocksStream handles requests for blocks
func (n *Node) handleBlocksStream(stream network.Stream) {
	remotePeer := stream.Conn().RemotePeer()
	log.Printf("Node %s: Received block request from %s",
		n.Host.ID().ShortString(), remotePeer.ShortString())

	// read request
	buf := make([]byte, 4096)
	bytesRead, err := stream.Read(buf)
	if err != nil && err != io.EOF {
		log.Printf("Node %s: Error reading block request: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	// unmarshal request
	var request RecoveryMessage
	if err := json.Unmarshal(buf[:bytesRead], &request); err != nil {
		log.Printf("Node %s: Failed to unmarshal block request: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	if request.Type != RequestBlock {
		log.Printf("Node %s: Unexpected message type in block request: %d",
			n.Host.ID().ShortString(), request.Type)
		stream.Reset()
		return
	}

	// get requested block
	n.Blockchain.mu.Lock()
	blockNode, exists := n.Blockchain.BlockIndex[request.BlockHash]
	n.Blockchain.mu.Unlock()

	if !exists {
		log.Printf("Node %s: Requested block %s not found",
			n.Host.ID().ShortString(), request.BlockHash[:8])

		response := RecoveryMessage{
			Type: ResponseBlock,
			Data: []byte{}, // Empty data to indicate block not found
		}

		responseBytes, _ := json.Marshal(response)
		stream.Write(responseBytes)
		stream.Close()
		return
	}

	// marshal block
	blockBytes, err := json.Marshal(blockNode.Block)
	if err != nil {
		log.Printf("Node %s: Failed to marshal block: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	// prepare response
	response := RecoveryMessage{
		Type: ResponseBlock,
		Data: blockBytes,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		log.Printf("Node %s: Failed to marshal block response: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	// send response
	if _, err := stream.Write(responseBytes); err != nil {
		log.Printf("Node %s: Failed to write block response: %v", n.Host.ID().ShortString(), err)
		stream.Reset()
		return
	}

	stream.Close()
}

// start the node's background processes (PubSub handler, Discovery, Mining loop)
func (n *Node) Start() {
	// start the PubSub message handler
	go n.pubsubHandler()

	// start Discovery processes
	dutil.Advertise(n.Ctx, n.Discovery, n.DiscoveryTag)
	log.Printf("Node %s advertising with tag %s\n", n.Host.ID().ShortString(), n.DiscoveryTag)
	go n.discoverPeers()

	recoveryNeeded := n.checkRecoveryNeeded()
	if recoveryNeeded {
		log.Printf("Node %s detected need for recovery, starting sync process...", n.Host.ID().ShortString())
		n.StartRecovery() // only start mining after recovery
	} else {
		log.Printf("Node %s: No recovery needed, blockchain is up to date or no peers available.", n.Host.ID().ShortString())
	}

	// go to the mining loop
	n.miningLoopWait.Add(1)
	go n.miningLoop()

	log.Printf("Node %s started successfully.", n.Host.ID().ShortString())
}

// stop gracefully shuts down the node's background processes and closes the host.
func (n *Node) Stop() {
	log.Printf("Stopping node %s...", n.Host.ID().ShortString())
	// cancel the context to signal goroutines to stop
	n.cancel()

	// wait for mining loop to finish if it's running
	n.miningLoopWait.Wait()

	// close PubSub Subscription and Topic
	if n.BlockSub != nil {
		n.BlockSub.Cancel()
	}
	if n.BlockTopic != nil {
		n.BlockTopic.Close()
	}

	// close the host
	if err := n.Host.Close(); err != nil {
		log.Printf("Error closing host for node %s: %v", n.Host.ID().ShortString(), err)
	}
	log.Printf("Node %s stopped.", n.Host.ID().ShortString())
}

// --- Libp2p Network Functions ---

// makeHost creates a new libp2p Host.
func makeHost(ctx context.Context, listenPort int) (host.Host, error) {
	listenAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)
	h, err := libp2p.New(
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.DefaultSecurity,
		libp2p.DefaultMuxers,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}
	log.Printf("Host created with ID: %s, listening on %s\n", h.ID().ShortString(), listenAddr)
	return h, nil
}

// setupDiscovery initializes the Kademlia DHT for peer discovery,
// focusing on local network discovery without connecting to default public bootstrap peers.
func setupDiscovery(ctx context.Context, h host.Host) (*drouting.RoutingDiscovery, error) {
	kademliaDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeServer) /*, dht.ProtocolPrefix("/myapp") // Optional: Use custom protocol prefix for private DHT */)
	if err != nil {
		return nil, fmt.Errorf("failed to create DHT: %w", err)
	}
	log.Printf("Node %s: DHT created.", h.ID().ShortString())

	// this bootstrapping is necessary to initialize the local routing table
	// and enable the DHT to function for local discovery.
	log.Printf("Node %s: Bootstrapping the DHT (for local operation)...", h.ID().ShortString())
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		return nil, fmt.Errorf("failed to bootstrap DHT: %w", err)
	}
	log.Printf("Node %s: DHT bootstrapped successfully.", h.ID().ShortString())

	// use a RoutingDiscovery utility to advertise this node and discover
	// other peers based on a shared rendezvous tag within the DHT network we form locally.
	routingDiscovery := drouting.NewRoutingDiscovery(kademliaDHT)

	log.Printf("Node %s: Routing Discovery initialized.", h.ID().ShortString())
	return routingDiscovery, nil
}

// requestChainInfo requests the height and tip hash from a peer
func (n *Node) requestChainInfo(peerId peer.ID) (int, string, error) {
	ctx, cancel := context.WithTimeout(n.Ctx, 50*time.Second)
	defer cancel()

	// open stream to peer
	stream, err := n.Host.NewStream(ctx, peerId, ChainInfoProtocolID)
	if err != nil {
		return 0, "", fmt.Errorf("failed to open stream to peer: %w", err)
	}
	defer stream.Close()

	// send empty request (just to initiate the info exchange)
	if _, err = stream.Write([]byte{}); err != nil {
		return 0, "", fmt.Errorf("failed to write to stream: %w", err)
	}

	// read response
	buf := make([]byte, 1024)
	bytesRead, err := stream.Read(buf)
	if err != nil {
		return 0, "", fmt.Errorf("failed to read from stream: %w", err)
	}

	// unmarshal response
	var response struct {
		Height  int
		TipHash string
	}

	if err := json.Unmarshal(buf[:bytesRead], &response); err != nil {
		return 0, "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return response.Height, response.TipHash, nil
}

// checkRecoveryNeeded determines if this node needs to recover by comparing
// its blockchain height with other peers in the network
func (n *Node) checkRecoveryNeeded() bool {
	// get list of connected peers
	peers := n.Host.Network().Peers()
	if len(peers) == 0 {
		log.Printf("Node %s: No peers to recover from\n", n.Host.ID().ShortString())
		return false // No peers to recover from
	}

	// get our current head height
	n.Blockchain.mu.Lock()
	localHeight := n.Blockchain.Head.Height
	n.Blockchain.mu.Unlock()

	// check if any peer has a longer chain
	for _, peerId := range peers {
		if peerId == n.Host.ID() {
			continue
		}

		// get peer's chain height
		height, _, err := n.requestChainInfo(peerId)
		if err != nil {
			log.Printf("Node %s: Failed to get chain info from peer %s: %v",
				n.Host.ID().ShortString(), peerId.ShortString(), err)
			continue
		}

		// if peer has longer chain, recovery is needed
		if height > localHeight {
			return true
		}
	}

	return false
}

// pubsubHandler processes messages received via PubSub topics.
func (n *Node) pubsubHandler() {
	log.Printf("Node %s starting PubSub handler for topic %s\n", n.Host.ID().ShortString(), n.BlockSub.Topic())
	failBuffer := []Block{}
	flag := false
	for {
		var receivedBlock Block
		if len(failBuffer) > 0 && flag {
			flag = false
			for i, block := range failBuffer {
				err := n.Blockchain.AddBlock(block)
				if err != nil {
					log.Printf("Node %s: Failed to add block from fail buffer: %v\n", n.Host.ID().ShortString(), err)
					continue
				}
				log.Printf("Node %s: Block %s added successfully from fail buffer\n",
					n.Host.ID().ShortString(), block.Hash[:8])
				failBuffer = append(failBuffer[:i], failBuffer[i+1:]...)
				flag = true
				break
			}
		} else {
			msg, err := n.BlockSub.Next(n.Ctx)
			// check for context cancellation first
			if n.Ctx.Err() != nil {
				log.Printf("Node %s: PubSub handler stopping due to context cancellation.\n", n.Host.ID().ShortString())
				return
			}
			if err != nil {
				log.Printf("Node %s: Error reading from pubsub topic %s: %v\n", n.Host.ID().ShortString(), n.BlockSub.Topic(), err)
				time.Sleep(1 * time.Second)
				continue
			}

			if msg.ReceivedFrom == n.Host.ID() {
				continue
			}

			err = json.Unmarshal(msg.Data, &receivedBlock)
			if err != nil {
				log.Printf("Node %s: Error unmarshalling block from pubsub from %s: %v\n", n.Host.ID().ShortString(), msg.GetFrom().ShortString(), err)
				continue
			}

			log.Printf("Node %s: Received block %s via PubSub from %s.\n", n.Host.ID().ShortString(), receivedBlock.Hash[:8], msg.GetFrom().ShortString())

			// try to add the block to our chain
			err = n.Blockchain.AddBlock(receivedBlock)
			if errors.Is(err, ErrOutOfRange) {
				log.Println("Block added to fail buffer")
				failBuffer = append(failBuffer, receivedBlock)
			} else if err != nil {
				log.Printf("Node %s: Failed to add block from pubsub: %v\n", n.Host.ID().ShortString(), err)

				// attempt fork resolution if the block looks valid but doesn't extend our chain
				if err.Error() == "block doesn't extend current tip (fork point detected)" {
					// error will not happen since we are using tree structure now
					// we will prune the chain instead eventually
					// pass
				}
			} else {
				// block added successfully
				flag = true
				log.Printf("Node %s: Block %s added successfully to chain\n",
					n.Host.ID().ShortString(), receivedBlock.Hash[:8])
			}
		}
	}
}

// discoverPeers continuously looks for peers using the discovery service.
func (n *Node) discoverPeers() {
	log.Printf("Node %s starting peer discovery...\n", n.Host.ID().ShortString())
	peerChan, err := n.Discovery.FindPeers(n.Ctx, n.DiscoveryTag)
	if err != nil {
		log.Printf("Node %s: Failed to start peer discovery: %v\n", n.Host.ID().ShortString(), err)
		return
	}

	for {
		select {
		case peerInfo := <-peerChan:
			if peerInfo.ID == n.Host.ID() || len(peerInfo.Addrs) == 0 {
				continue
			}
			log.Printf("Node %s: Discovered peer %s\n", n.Host.ID().ShortString(), peerInfo.ID.ShortString())
			// Attempt connection only if not already connected
			if n.Host.Network().Connectedness(peerInfo.ID) != network.Connected {
				log.Printf("Node %s: Attempting connection to %s...\n", n.Host.ID().ShortString(), peerInfo.ID.ShortString())
				if err := n.Host.Connect(n.Ctx, peerInfo); err != nil {
					log.Printf("Node %s: Error connecting to peer %s: %s\n", n.Host.ID().ShortString(), peerInfo.ID.ShortString(), err)
				} else {
					log.Printf("Node %s: Connected to peer: %s\n", n.Host.ID().ShortString(), peerInfo.ID.ShortString())
					// Optional: Trigger state sync request here after connecting
				}
			} else {
				// log.Printf("Node %s: Already connected to peer %s\n", n.Host.ID().ShortString(), peerInfo.ID.ShortString())
			}
		case <-n.Ctx.Done():
			log.Printf("Node %s: Stopping peer discovery due to context cancellation.\n", n.Host.ID().ShortString())
			return
		}
	}
}
