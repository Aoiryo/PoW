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

// BlockHeader defines the block header information
type BlockHeader struct {
	ParentHash string `json:"parentHash"`
	MerkleRoot string `json:"merkleRoot"` // Simplified: can be the hash of the content
	Timestamp  int64  `json:"timestamp"`
	Difficulty int    `json:"difficulty"` // PoW difficulty target
	Nonce      int64  `json:"nonce"`
	Height     int    `json:"height"`
}

// Block defines the block structure
type Block struct {
	Header  BlockHeader `json:"header"`
	Content string      `json:"content"` // Simplified: should be a list of transactions in practice
	Hash    string      `json:"hash"`    // Hash of the current block
}

// Blockchain defines the blockchain structure (maintained locally by each node)
type Blockchain struct {
	mu            sync.RWMutex
	Chain         []Block
	Mempool       []string         // Pending content (transactions)
	blockIndex    map[string]int   // Maps block hash to its index in the Chain
	pendingBlocks map[string]Block // Stores blocks that couldn't be added to the main chain yet
}

// Node represents a node in the network (miner or client) using libp2p
type Node struct {
	Host           host.Host
	Blockchain     *Blockchain
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
const BlockProtocolID = "/blockchain/blocks/1.0.0"
const BlockTopicName = "blockchain/blocks" // Using topic name as discovery tag

// --- Blockchain Methods ---

// CalculateHash computes the hash of the block
func (b *Block) CalculateHash() string {
	headerBytes, _ := json.Marshal(b.Header)
	return fmt.Sprintf("%x", sha256.Sum256(headerBytes))
}

// NewBlock creates a new block (called after successful mining)
func NewBlock(content string, parentBlock Block, difficulty int) *Block {
	header := BlockHeader{
		ParentHash: parentBlock.Hash,
		MerkleRoot: fmt.Sprintf("%x", sha256.Sum256([]byte(content))), // Simplified
		Timestamp:  time.Now().Unix(),
		Difficulty: difficulty,
		Nonce:      0,
		Height:     parentBlock.Header.Height + 1, // Set height based on parent
	}
	block := &Block{
		Header:  header,
		Content: content,
	}
	return block
}

// removeFromMempool removes a content from the mempool
func removeFromMempool(mempool []string, content string) []string {
	for i, c := range mempool {
		if c == content {
			return append(mempool[:i], mempool[i+1:]...)
		}
	}
	return mempool
}

// AddBlock adds a block to the local chain
func (bc *Blockchain) AddBlock(block Block) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if len(bc.Chain) == 0 {
		return fmt.Errorf("cannot add block to empty chain (genesis block should exist)")
	}

	// check if the block extends the current chain tip
	parentBlock := bc.Chain[len(bc.Chain)-1]
	if block.Header.ParentHash != parentBlock.Hash {
		// block doesn't extend current tip - could be fork or orphan
		// check if we know about the parent block
		_, parentExists := bc.blockIndex[block.Header.ParentHash]
		if !parentExists {
			// we don't know about the parent - store in pending
			bc.pendingBlocks[block.Hash] = block
			return fmt.Errorf("unknown parent block: %s", block.Header.ParentHash)
		}

		// parent exists but isn't the tip - potential fork point
		// this will be handled by the ResolveFork function
		return fmt.Errorf("block doesn't extend current tip (fork point detected)")
	}

	if !bc.isValidBlock(block) { // Call validation method
		return fmt.Errorf("invalid block received: %s...", block.Hash[:8])
	}

	// add valid block to chain
	bc.Chain = append(bc.Chain, block)
	bc.blockIndex[block.Hash] = len(bc.Chain) - 1

	// TODO: clean up Mempool for confirmed content
	bc.Mempool = removeFromMempool(bc.Mempool, block.Content)

	log.Printf("Block %s... (H:%d) added to the chain.\n", block.Hash[:8], block.Header.Height)
	return nil
}

// isValidBlock validates a single block relative to the current chain state
func (bc *Blockchain) isValidBlock(block Block) bool {
	if len(bc.Chain) == 0 {
		log.Println("Validation Warning: Cannot validate against empty chain.")
		// this check might be redundant if AddBlock already checks len > 0
		return false
	}
	parentBlock := bc.Chain[len(bc.Chain)-1]
	if block.Header.ParentHash != parentBlock.Hash {
		log.Printf("Validation Error: Parent hash mismatch. Expected %s..., got %s...\n", parentBlock.Hash[:8], block.Header.ParentHash[:8])
		return false
	}

	targetPrefix := ""
	for i := 0; i < block.Header.Difficulty; i++ {
		targetPrefix += "0"
	}
	calculatedHash := block.CalculateHash()
	if calculatedHash != block.Hash || calculatedHash[:block.Header.Difficulty] != targetPrefix {
		log.Printf("Validation Error: PoW or Hash mismatch for block %s...\n", block.Hash[:8])
		return false
	}
	// and more...
	return true
}

// isBlockValidIntrinsic validates a block against a specified parent block
// This is useful during fork resolution when we need to validate against
// a specific parent rather than the chain tip
func (bc *Blockchain) isBlockValidIntrinsic(block Block, parentBlock Block) bool {
	// validate parent hash
	if block.Header.ParentHash != parentBlock.Hash {
		log.Printf("Validation Error: Parent hash mismatch. Expected %s..., got %s...\n", parentBlock.Hash[:8], block.Header.ParentHash[:8])
		return false
	}

	// validate height is one more than parent
	if block.Header.Height != parentBlock.Header.Height+1 {
		log.Printf("Validation Error: Height mismatch. Expected %d, got %d\n", parentBlock.Header.Height+1, block.Header.Height)
		return false
	}

	// validate PoW
	targetPrefix := ""
	for i := 0; i < block.Header.Difficulty; i++ {
		targetPrefix += "0"
	}
	calculatedHash := block.CalculateHash()
	if calculatedHash != block.Hash || calculatedHash[:block.Header.Difficulty] != targetPrefix {
		log.Printf("Validation Error: PoW or Hash mismatch for block %s...\n", block.Hash[:8])
		return false
	}

	// additional validations could include:
	// - timestamp validations (block time can't be too far in future)
	// - content validations
	// - signature validations (for PoS chains, maybe we don't need this)

	return true
}

// IsValidChain validates an entire chain structure (e.g., a received chain)
func (bc *Blockchain) IsValidChain(chain []Block) bool {
	if len(chain) == 0 {
		return false
	}
	// TODO: Validate Genesis Block

	for i := 1; i < len(chain); i++ {
		currentBlock := chain[i]
		prevBlock := chain[i-1]
		if currentBlock.Header.ParentHash != prevBlock.Hash {
			return false
		}
		targetPrefix := ""
		for j := 0; j < currentBlock.Header.Difficulty; j++ {
			targetPrefix += "0"
		}
		calculatedHash := currentBlock.CalculateHash()
		if calculatedHash != currentBlock.Hash || calculatedHash[:currentBlock.Header.Difficulty] != targetPrefix {
			return false
		}
	}
	return true
}

// ResolveFork attempts to switch to a new chain branch if it's longer/better.
// Called when a received block is valid but doesn't extend the current tip.
// Returns true if a reorganization occurred, false otherwise.
func (n *Node) ResolveFork(newBlock Block) (bool, error) {
	n.Blockchain.mu.Lock()
	defer n.Blockchain.mu.Unlock()

	log.Printf("Attempting to resolve fork with new block %s... (H:%d)\n", newBlock.Hash[:8], newBlock.Header.Height)

	currentTip := n.Blockchain.Chain[len(n.Blockchain.Chain)-1]

	// basic check: compare heights (proxy for longest chain)
	if newBlock.Header.Height <= currentTip.Header.Height {
		log.Printf("New block height %d is not greater than current tip height %d. Fork not resolved.\n", newBlock.Header.Height, currentTip.Header.Height)
		// store it as pending in case its children make it longer la	ster
		n.Blockchain.pendingBlocks[newBlock.Hash] = newBlock
		return false, nil // Current chain is longer or equal
	}

	// simplified reorg (assumes newBlock's parent IS in current chain)
	parentIndex, parentExists := n.Blockchain.blockIndex[newBlock.Header.ParentHash]
	if !parentExists {
		// this shouldn't happen if AddBlock already checked parent existence for non-tip extensions
		log.Printf("ResolveFork Error: Parent %s... of new block %s... not found in main chain index, storing as pending.", newBlock.Header.ParentHash[:8], newBlock.Hash[:8])
		n.Blockchain.pendingBlocks[newBlock.Hash] = newBlock
		return false, fmt.Errorf("parent block not found during fork resolution attempt")
	}

	// validate the new block itself against its actual parent in our chain
	parentBlock := n.Blockchain.Chain[parentIndex]
	if !n.Blockchain.isBlockValidIntrinsic(newBlock, parentBlock) {
		log.Printf("ResolveFork Error: New block %s... failed intrinsic validation against parent %s...", newBlock.Hash[:8], parentBlock.Hash[:8])
		return false, fmt.Errorf("new block failed validation during fork resolution")
	}

	log.Printf("New block's chain (H:%d) is longer than current tip (H:%d). Performing reorganization.\n", newBlock.Header.Height, currentTip.Header.Height)

	// reorganize the chain
	// remove future blocks from main chain (stale blocks)
	staleBlocks := n.Blockchain.Chain[parentIndex+1:]

	// truncate the chain back to the common parent
	n.Blockchain.Chain = n.Blockchain.Chain[:parentIndex+1]

	// remove stale blocks from index
	for _, staleBlock := range staleBlocks {
		delete(n.Blockchain.blockIndex, staleBlock.Hash)
		log.Printf("Removed stale block %s... (H:%d) from main chain.\n", staleBlock.Hash[:8], staleBlock.Header.Height)
		// TODO: add transactions from stale blocks back to Mempool
	}

	// add the new block
	n.Blockchain.Chain = append(n.Blockchain.Chain, newBlock)
	n.Blockchain.blockIndex[newBlock.Hash] = len(n.Blockchain.Chain) - 1
	log.Printf("Added new block %s... (H:%d) to main chain during reorg. New chain length: %d\n", newBlock.Hash[:8], newBlock.Header.Height, len(n.Blockchain.Chain))

	// TODO: check pendingBlocks again, as this reorg might make some addable

	return true, nil // reorg occurred
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
	genesisHeader := BlockHeader{Timestamp: time.Now().Unix(), Difficulty: 3, Height: 0}
	genesisBlock := Block{Header: genesisHeader, Content: "Genesis Block"}
	genesisBlock.Hash = genesisBlock.CalculateHash()

	// 3. initialize Blockchain
	blockchain := &Blockchain{Chain: []Block{genesisBlock}, Mempool: []string{}, blockIndex: make(map[string]int), pendingBlocks: make(map[string]Block)}

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

// MineBlock picks a random nonce and increases it to find a valid one
func (n *Node) MineBlock() (*Block, error) {
	n.Blockchain.mu.RLock()
	if len(n.Blockchain.Mempool) == 0 {
		n.Blockchain.mu.RUnlock()
		return nil, fmt.Errorf("mempool is empty")
	}
	content := n.Blockchain.Mempool[0]
	parentBlock := n.Blockchain.Chain[len(n.Blockchain.Chain)-1]
	difficulty := parentBlock.Header.Difficulty
	n.Blockchain.mu.RUnlock()

	block := NewBlock(content, parentBlock, difficulty)
	targetPrefix := ""
	for i := 0; i < difficulty; i++ {
		targetPrefix += "0"
	}

	log.Printf("Node %s started mining...\n", n.Host.ID().ShortString())
	startTime := time.Now()
	// set nonce as a random number to avoid collisions
	nonce := rand.Int63n(1000000)
	for {
		select {
		case <-n.Ctx.Done():
			log.Printf("Node %s mining cancelled.\n", n.Host.ID().ShortString())
			return nil, fmt.Errorf("mining cancelled")
		default:
			block.Header.Nonce = nonce
			hash := block.CalculateHash()
			if hash[:difficulty] == targetPrefix {
				block.Hash = hash
				duration := time.Since(startTime)
				log.Printf("Node %s found block! Hash: %s..., Nonce: %d, Time: %s\n", n.Host.ID().ShortString(), hash[:8], nonce, duration)
				n.Blockchain.mu.Lock()
				if len(n.Blockchain.Mempool) > 0 && n.Blockchain.Mempool[0] == content {
					n.Blockchain.Mempool = n.Blockchain.Mempool[1:]
				}
				n.Blockchain.mu.Unlock()
				return block, nil
			}
			nonce++
		}
	}
}

// SubmitContent adds content to the node's mempool
func (n *Node) SubmitContent(content string) error {
	n.Blockchain.mu.Lock()
	n.Blockchain.Mempool = append(n.Blockchain.Mempool, content)
	n.Blockchain.mu.Unlock()
	log.Printf("Node %s added content to mempool: %s\n", n.Host.ID().ShortString(), content)
	// broadcast transaction here via another PubSub topic or direct messages
	return nil
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

			// Attempt fork resolution if the block looks valid but doesn't extend our chain
			if err.Error() == "block doesn't extend current tip (fork point detected)" {
				resolved, resolveErr := n.ResolveFork(receivedBlock)
				if resolveErr != nil {
					log.Printf("Node %s: Fork resolution failed: %v\n", n.Host.ID().ShortString(), resolveErr)
				} else if resolved {
					log.Printf("Node %s: Successfully resolved fork with block %s\n",
						n.Host.ID().ShortString(), receivedBlock.Hash[:8])
				}
			}
		} else {
			// Block added successfully
			log.Printf("Node %s: Block %s added successfully to chain\n",
				n.Host.ID().ShortString(), receivedBlock.Hash[:8])
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
			n.Blockchain.mu.RLock()
			mempoolSize := len(n.Blockchain.Mempool)
			n.Blockchain.mu.RUnlock()

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

// GetChainTipHash returns the hash of the latest block in the chain
// tip: the last block in the chain (think about tree structure)
func (n *Node) GetChainTipHash() string {
	n.Blockchain.mu.RLock()
	defer n.Blockchain.mu.RUnlock()
	if len(n.Blockchain.Chain) == 0 {
		return ""
	}
	return n.Blockchain.Chain[len(n.Blockchain.Chain)-1].Hash
}
