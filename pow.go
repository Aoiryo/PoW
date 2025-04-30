package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"github.com/libp2p/go-libp2p"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

// --- Core Data Structures ---

var difficulty = 4 // Difficulty level for PoW

type transaction struct {
	Content   string
	Timestamp time.Time
}

type BlochToHash struct {
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

// // BlockHeader defines the block header information
// type BlockHeader struct {
// 	ParentHash string `json:"parentHash"`
// 	MerkleRoot string `json:"merkleRoot"` // Simplified: can be the hash of the content
// 	Timestamp  int64  `json:"timestamp"`
// 	Difficulty int    `json:"difficulty"` // PoW difficulty target
// 	Nonce      int64  `json:"nonce"`
// 	Height     int    `json:"height"`
// }

// // Block defines the block structure
// type Block struct {
// 	Header  BlockHeader `json:"header"`
// 	Content string      `json:"content"` // Simplified: should be a list of transactions in practice
// 	Hash    string      `json:"hash"`    // Hash of the current block
// }

// // Blockchain defines the blockchain structure (maintained locally by each node)
// type Blockchain struct {
// 	mu            sync.RWMutex
// 	Chain         []Block
// 	Mempool       []string         // Pending content (transactions)
// 	blockIndex    map[string]int   // Maps block hash to its index in the Chain
// 	pendingBlocks map[string]Block // Stores blocks that couldn't be added to the main chain yet
// }

// // Node represents a node in the network (miner or client) using libp2p
// type Node struct {
// 	Host           host.Host
// 	Blockchain     *Blockchain
// 	PubSub         *pubsub.PubSub
// 	BlockTopic     *pubsub.Topic
// 	BlockSub       *pubsub.Subscription       // Added field for the subscription
// 	Discovery      *drouting.RoutingDiscovery // Added field for discovery
// 	Ctx            context.Context
// 	cancel         context.CancelFunc // To allow stopping node-specific goroutines
// 	DiscoveryTag   string             // Tag used for finding peers
// 	miningLoopWait sync.WaitGroup     // To wait for mining loop to finish
// }

// --- Constants for libp2p ---
const BlockProtocolID = "/blockchain/blocks/1.0.0"
const BlockTopicName = "blockchain/blocks" // Using topic name as discovery tag

// --- Client Submission Methods ---
// SubmitContent adds content to the node's mempool
func (n *Node) SubmitContent(content string) error {
	transaction := transaction{
		Content:   content,
		Timestamp: time.Now(),
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Mempool = append(n.Mempool, transaction)

	log.Printf("Node %s added content to mempool: %s\n", n.Host.ID().ShortString(), content)
	// broadcast transaction here via another PubSub topic or direct messages
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
					log.Printf("Node %s: Heads updated to %s added successfully to chain\n", n.Host.ID().ShortString(), n.Blockchain.Head.Block.Hash[:8])
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
	// n.Blockchain.mu.RLock()
	// if len(n.Blockchain.Mempool) == 0 {
	// 	n.Blockchain.mu.RUnlock()
	// 	return nil, fmt.Errorf("mempool is empty")
	// }

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
				// n.Blockchain.mu.Lock()

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
		Timestamp: time.Now().Round(0),
		Data:      transaction.Content,
		PreHash:   bc.Head.Block.Hash,
		Nonce:     nonce,
	}
	return block
}

// CalculateHash computes the hash of the block
func (b *Block) CalculateHash() string {

	blockToHash := BlochToHash{
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

	blocknode := &BlockNode{
		Block:    block,
		Parent:   bc.BlockIndex[block.PreHash],
		Children: []*BlockNode{},
		Height:   block.Index,
	}

	if len(bc.BlockIndex) == 0 {
		return fmt.Errorf("cannot add block to empty chain (genesis block should exist)")
	}

	if !bc.isValidBlock(blocknode.Block) {
		return fmt.Errorf("block %s is invalid", blocknode.Block.Hash)
	}

	prevhash := blocknode.Block.PreHash
	// if prevhash is not in the chain, it's invalid
	if _, ok := bc.BlockIndex[prevhash]; !ok {
		return fmt.Errorf("block %s is invalid (parent block not found)", blocknode.Block.Hash)
	}

	// if block with same data and timestamp exists, it's a duplicate
	if !bc.DeDuplicate(blocknode) {
		return fmt.Errorf("block %s is a duplicate", blocknode.Block.Hash)
	}

	// if prevhash is in the chain and not a duplicate, add it to the chain
	bc.BlockIndex[prevhash].Children = append(bc.BlockIndex[prevhash].Children, blocknode)
	bc.BlockIndex[blocknode.Block.Hash] = blocknode
	blocknode.Parent = bc.BlockIndex[prevhash]

	// if it updated the longest chain, update the head
	if blocknode.Height > bc.Head.Height {
		bc.Head = blocknode
	}

	// if it parents previously a tip, update the tips
	delete(bc.Tips, prevhash)
	bc.Tips[blocknode.Block.Hash] = blocknode

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
func (bc *Blockchain) isValidBlock(block Block) bool {

	targetPrefix := ""
	for i := 0; i < difficulty; i++ {
		targetPrefix += "0"
	}
	calculatedHash := block.CalculateHash()
	if calculatedHash != block.Hash || calculatedHash[:difficulty] != targetPrefix {
		log.Printf("Validation Error: PoW or Hash mismatch for block %s...\n", block.Hash[:8])
		return false
	}
	// and more...
	return true
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
	sub, err := blockTopic.Subscribe()
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
	h.SetStreamHandler(BlockProtocolID, node.handleStream)

	return node, nil
}

// start the node's background processes (PubSub handler, Discovery, Mining loop)
func (n *Node) Start() {
	// start the PubSub message handler
	go n.pubsubHandler()

	// start Discovery processes
	dutil.Advertise(n.Ctx, n.Discovery, n.DiscoveryTag)
	log.Printf("Node %s advertising with tag %s\n", n.Host.ID().ShortString(), n.DiscoveryTag)
	go n.discoverPeers()

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

// handleStream handles incoming direct streams (e.g., for block requests)
func (n *Node) handleStream(stream network.Stream) {
	log.Printf("Node %s: Received new stream from %s\n", n.Host.ID().ShortString(), stream.Conn().RemotePeer().ShortString())
	// TODO: may need to handle direct messages (e.g., request for blocks/headers)
	// just log and close now
	stream.Close()
}

// pubsubHandler processes messages received via PubSub topics.
func (n *Node) pubsubHandler() {
	log.Printf("Node %s starting PubSub handler for topic %s\n", n.Host.ID().ShortString(), n.BlockSub.Topic())
	for {
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

		var receivedBlock Block
		err = json.Unmarshal(msg.Data, &receivedBlock)
		if err != nil {
			log.Printf("Node %s: Error unmarshalling block from pubsub from %s: %v\n", n.Host.ID().ShortString(), msg.GetFrom().ShortString(), err)
			continue
		}

		log.Printf("Node %s: Received block %s via PubSub from %s.\n", n.Host.ID().ShortString(), receivedBlock.Hash[:8], msg.GetFrom().ShortString())

		// Try to add the block to our chain
		err = n.Blockchain.AddBlock(receivedBlock)
		if err != nil {
			log.Printf("Node %s: Failed to add block from pubsub: %v\n", n.Host.ID().ShortString(), err)

			// // Attempt fork resolution if the block looks valid but doesn't extend our chain
			// if err.Error() == "block doesn't extend current tip (fork point detected)" {
			// 	resolved, resolveErr := n.ResolveFork(receivedBlock)
			// 	if resolveErr != nil {
			// 		log.Printf("Node %s: Fork resolution failed: %v\n", n.Host.ID().ShortString(), resolveErr)
			// 	} else if resolved {
			// 		log.Printf("Node %s: Successfully resolved fork with block %s\n",
			// 			n.Host.ID().ShortString(), receivedBlock.Hash[:8])
			// 	}
			// }
		} else {
			// Block added successfully
			log.Printf("Node %s: Block %s added successfully to chain\n",
				n.Host.ID().ShortString(), receivedBlock.Hash[:8])
			log.Printf("Node %s: Heads updated to %s added successfully to chain\n", n.Host.ID().ShortString(), n.Blockchain.Head.Block.Hash[:8])
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
