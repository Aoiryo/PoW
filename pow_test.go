package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	// Use testify for better assertions if desired:
	// "github.com/stretchr/testify/assert"
	// "github.com/stretchr/testify/require"
)

var (
	ErrBlockNotExtendTip = errors.New("block does not extend current tip")
	ErrBlockInvalid      = errors.New("invalid block")
)

func TestBlockchainSetup(t *testing.T) {
	// create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a node
	node, err := NewNode(ctx, 0, "test-single-node")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	node.Start()
	defer node.Stop()

	// submit content and mine a block
	err = node.SubmitContent("Test content for mining1")
	if err != nil {
		t.Fatalf("Failed to submit content: %v", err)
	}
	err = node.SubmitContent("Test content for mining2")
	if err != nil {
		t.Fatalf("Failed to submit content: %v", err)
	}
	err = node.SubmitContent("Test content for mining3")
	if err != nil {
		t.Fatalf("Failed to submit content: %v", err)
	}

	// Wait for mining
	time.Sleep(30 * time.Second)

	// check that the blockchain has grown
	node.Blockchain.mu.Lock()
	chainLen := node.Blockchain.Head.Height
	node.Blockchain.mu.Unlock()

	if chainLen == 0 {
		t.Errorf("Expected chain to grow beyond genesis, but length is %d", chainLen)
	} else {
		fmt.Println("Blockchain has grown successfully, length is ", chainLen)
		t.Logf("Chain has grown to length %d", chainLen)
	}
}

func TestBlockchainMultiSetup(t *testing.T) {
	// create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create a node
	node, err := NewNode(ctx, 0, "test-multi-node")
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// submit content and mine a block
	err = node.SubmitContent("Test content for mining1")
	if err != nil {
		t.Fatalf("Failed to submit content: %v", err)
	}
	err = node.SubmitContent("Test content for mining2")
	if err != nil {
		t.Fatalf("Failed to submit content: %v", err)
	}
	err = node.SubmitContent("Test content for mining3")
	if err != nil {
		t.Fatalf("Failed to submit content: %v", err)
	}

	// Wait for mining
	time.Sleep(30 * time.Second)

	// check that the blockchain has grown
	node.Blockchain.mu.Lock()
	chainLen := node.Blockchain.Head.Height
	node.Blockchain.mu.Unlock()

	if chainLen == 0 {
		t.Errorf("Expected chain to grow beyond genesis, but length is %d", chainLen)
	} else {
		fmt.Println("Blockchain has grown successfully, length is ", chainLen)
		t.Logf("Chain has grown to length %d", chainLen)
	}
}

const (
	// Using a dynamic tag per test run to minimize interference if tests run quasi-parallel locally
	testDiscoveryTagBase = "blockchain-test-network"
	testTimeout          = 500 * time.Second // Increased timeout for potentially complex scenarios
	setupWaitTime        = 10 * time.Second  // Time for initial node startup and basic discovery
	syncWaitTime         = 20 * time.Second  // Increased time for sync/propagation after events
	miningWaitTime       = 25 * time.Second  // Increased time to allow for mining cycles
)

// Helper function to create a unique discovery tag for each test run
func getTestDiscoveryTag(t *testing.T) string {
	// Using test name and timestamp to create a more unique tag
	// Note: t.Name() can contain characters like '/', replacing them.
	// This helps avoid different test runs interfering via DHT if run close together.
	// return fmt.Sprintf("%s-%s-%d", testDiscoveryTagBase, strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano())
	// Simpler version for now:
	return fmt.Sprintf("%s-%d", testDiscoveryTagBase, time.Now().UnixNano())
}

// helper function to set up a network of nodes for testing
func setupTestNetwork(t *testing.T, numNodes int, discoveryTag string) (context.Context, context.CancelFunc, []*Node, error) {
	t.Helper() // Marks this as a test helper function
	// use a test-specific context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)

	nodes := make([]*Node, numNodes) // pre-allocate slice
	var nodesMutex sync.Mutex        // mutex to protect access to the nodes slice
	var startWg sync.WaitGroup
	var firstError error
	var errMutex sync.Mutex

	log.Printf("[%s] Setting up test network with %d nodes (Tag: %s)...", t.Name(), numNodes, discoveryTag)
	startWg.Add(numNodes)

	for i := 0; i < numNodes; i++ {
		go func(nodeIndex int) {
			defer startWg.Done()
			// port 0: let the OS choose an available port
			node, err := NewNode(ctx, 0, discoveryTag)
			if err != nil {
				errMutex.Lock()
				log.Printf("[%s] Node %d failed to start: %v", t.Name(), nodeIndex, err)
				if firstError == nil {
					firstError = fmt.Errorf("node %d failed to start: %w", nodeIndex, err)
				}
				errMutex.Unlock()
				return
			}
			node.Start()

			nodesMutex.Lock()
			nodes[nodeIndex] = node
			nodesMutex.Unlock()

			log.Printf("[%s] Node %d (%s) started on %s", t.Name(), nodeIndex, node.Host.ID().ShortString(), node.Host.Addrs()[0]) // Assuming at least one address

		}(i)
		time.Sleep(100 * time.Millisecond)
	}

	startWg.Wait()

	if firstError != nil {
		log.Printf("[%s] Setup failed, stopping any started nodes.", t.Name())
		nodesMutex.Lock()
		for _, node := range nodes {
			if node != nil {
				go node.Stop()
			}
		}
		nodesMutex.Unlock()
		cancel() // cancel context if setup failed
		return nil, nil, nil, fmt.Errorf("failed to set up test network: %w", firstError)
	}

	// verify all nodes started (check for nils in the slice)
	nodesMutex.Lock()
	finalNodes := make([]*Node, 0, numNodes)
	for i := 0; i < numNodes; i++ {
		if nodes[i] == nil {
			// this case should ideally be caught by firstError, but as a safeguard:
			log.Printf("[%s] Setup inconsistency: Node %d is nil despite no recorded error.", t.Name(), i)
			for _, node := range nodes { // Stop already started nodes
				if node != nil {
					go node.Stop()
				}
			}
			cancel()
			nodesMutex.Unlock()
			return nil, nil, nil, fmt.Errorf("node %d failed to initialize properly", i)
		}
		finalNodes = append(finalNodes, nodes[i]) // collect non-nil nodes
	}
	nodesMutex.Unlock()

	log.Printf("[%s] All %d nodes started. Allowing %v for discovery...", t.Name(), numNodes, setupWaitTime)
	time.Sleep(setupWaitTime) // allow time for nodes to discover each other

	return ctx, cancel, finalNodes, nil
}

// helper to check if all nodes have converged to the same chain tip hash and height
func checkChainConvergence(t *testing.T, nodes []*Node) (converged bool, tipHash string, tipHeight int) {

	t.Helper()
	if len(nodes) <= 1 {
		return true, "", -1 // 0 or 1 node, guaranteed convergence
	}

	nodes[0].Blockchain.mu.Lock()
	defer nodes[0].Blockchain.mu.Unlock()

	// check if all nodes have same head block
	for i := 1; i < len(nodes); i++ {
		nodes[i].Blockchain.mu.Lock()
		defer nodes[i].Blockchain.mu.Unlock()
		if nodes[i].Blockchain.Head.Block != nodes[0].Blockchain.Head.Block {
			b := nodes[i].Blockchain.Head.Block
			fmt.Printf("Node %d has head block: hash: %s, index: %d, data: %s, nonce: %d, prevHash: %s, time: %v.\n", i, b.Hash[:8], b.Index, b.Data, b.Nonce, b.PreHash, b.Timestamp)
			b = nodes[0].Blockchain.Head.Block
			fmt.Printf("Node %d has head block: hash: %s, index: %d, data: %s, nonce: %d, prevHash: %s, time: %v.\n", 0, b.Hash[:8], b.Index, b.Data, b.Nonce, b.PreHash, b.Timestamp)
			t.Logf("[%s] Convergence check failed: Node %d has different head block.", t.Name(), i)

			return false, "", -1
		}
	}
	fmt.Println("All nodes have the same head block.")

	headList := make([]*BlockNode, len(nodes))
	for i, node := range nodes {
		headList[i] = node.Blockchain.Head
	}

	for {
		genesisCnt := 0
		template := headList[0].Block
		headList[0] = headList[0].Parent
		if headList[0] == nil {
			genesisCnt++
		}

		for i := 1; i < len(headList); i++ {
			if headList[i].Block != template {
				t.Logf("[%s] Convergence check failed: Node %d has different head block.", t.Name(), i)
				return false, "", -1
			}
			headList[i] = headList[i].Parent
			if headList[i] == nil {
				genesisCnt++
			}
		}
		if genesisCnt > 0 {
			if genesisCnt == len(headList) {
				fmt.Println("All nodes have the same block chain.")
				return true, nodes[0].Blockchain.Head.Block.Hash, nodes[0].Blockchain.Head.Height
			} else {
				t.Logf("[%s] Convergence check failed: Nodes have different chain lengths.", t.Name())
				return false, "", -1
			}
		}
	}
}

// Helper to wait for network convergence with retries
func waitForConvergence(t *testing.T, nodes []*Node, maxWait time.Duration, checkInterval time.Duration) (bool, string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), maxWait)
	defer cancel()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Logf("[%s] Timed out waiting for convergence after %v", t.Name(), maxWait)
			// Check one last time
			converged, tipHash, tipHeight := checkChainConvergence(t, nodes)
			if !converged {
				t.Errorf("[%s] Failed to converge within %v", t.Name(), maxWait)
			}
			return converged, tipHash, tipHeight
		case <-ticker.C:
			converged, tipHash, tipHeight := checkChainConvergence(t, nodes)
			if converged {
				t.Logf("[%s] Convergence achieved.", t.Name())
				return true, tipHash, tipHeight
			}
			// Not converged yet, continue loop
		}
	}
}

func ConnectNodes(nodes []*Node, ctx context.Context) error {
	// Connect all nodes to each other
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			addrInfo := peer.AddrInfo{
				ID:    nodes[j].Host.ID(),
				Addrs: nodes[j].Host.Addrs(),
			}
			if err := nodes[i].Host.Connect(ctx, addrInfo); err != nil {
				log.Printf("Failed to connect Node %d to Node %d: %v", i, j, err)
				return fmt.Errorf("failed to connect Node %d to Node %d: %w", i, j, err)
			} else {
				log.Printf("Node %d connected to Node %d", i, j)
			}
		}
	}
	return nil
}

// helper function to find content in the chain
func findContentInChain(t *testing.T, node *Node, content string, startHeight int, endHeight int) bool {
	t.Helper()
	node.Blockchain.mu.Lock()
	defer node.Blockchain.mu.Unlock()

	if endHeight < startHeight || node.Blockchain.Head == nil {
		return false
	}

	current := node.Blockchain.Head
	for current != nil && current.Height >= startHeight {
		if current.Height <= endHeight {
			// Adjust check based on Block.Data type and structure
			if strings.Contains(fmt.Sprintf("%v", current.Block.Data), content) { // Example check
				log.Printf("[%s] Found content '%s' in block %d (Hash: %s...)", t.Name(), content, current.Height, current.Block.Hash[:8])
				return true // Content found
			}
		}
		if current.Height < startHeight || current.Parent == nil {
			break
		}
		current = current.Parent
	}

	log.Printf("[%s] Content '%s' not found in blocks between height %d and %d", t.Name(), content, startHeight, endHeight)
	return false // Content not found
}

// helper for setting up a tree structure with forked chains
func nodesForkSetup(nodes []*Node) {
	// Connect all nodes to each other
	block1 := Block{
		Index:     1,
		Data:      "This is block1",
		Nonce:     1,
		PreHash:   nodes[0].Blockchain.Head.Block.Hash,
		Timestamp: time.Now().Truncate(time.Minute).Round(0),
	}
	block1.Hash = block1.CalculateHash()

	block2 := Block{
		Index:     1,
		Data:      "This is block2",
		Nonce:     2,
		PreHash:   nodes[0].Blockchain.Head.Block.Hash,
		Timestamp: time.Now().Truncate(time.Minute).Round(0),
	}
	block2.Hash = block2.CalculateHash()

	for i := 0; i < len(nodes); i++ {
		block1Node := &BlockNode{
			Block:    block1,
			Parent:   nodes[i].Blockchain.Head,
			Children: []*BlockNode{},
			Height:   1,
		}
		block2Node := &BlockNode{
			Block:    block2,
			Parent:   nodes[i].Blockchain.Head,
			Children: []*BlockNode{},
			Height:   1,
		}
		nodes[i].Blockchain.mu.Lock()
		nodes[i].Blockchain.Head.Children = append(nodes[i].Blockchain.Head.Children, block1Node, block2Node)
		nodes[i].Blockchain.BlockIndex[block1.Hash] = block1Node
		nodes[i].Blockchain.BlockIndex[block2.Hash] = block2Node
		delete(nodes[i].Blockchain.BlockIndex, nodes[i].Blockchain.Head.Block.Hash)
		nodes[i].Blockchain.Tips[block1.Hash] = block1Node
		nodes[i].Blockchain.Tips[block2.Hash] = block2Node
		j := rand.Intn(2)
		if j == 0 {
			nodes[i].Blockchain.Head = block1Node
		} else {
			nodes[i].Blockchain.Head = block2Node
		}
		nodes[i].Blockchain.mu.Unlock()
	}
}

// --- Test Cases ---
// Test Case 1: Single Miner, Broadcast, and Convergence Verification
func TestSingleMinerBroadcastAndConvergence(t *testing.T) {
	t.Parallel()
	log.Println("--- TestSingleMinerBroadcastAndConvergence ---")
	discoveryTag := getTestDiscoveryTag(t)
	ctx, cancel, nodes, err := setupTestNetwork(t, 3, discoveryTag) // setup 3 nodes
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer cancel()
	defer func() { /* ... (defer cleanup remains the same) ... */ }()

	if len(nodes) < 3 {
		t.Fatal("Test requires at least 3 nodes.") // Sanity check
	}

	// connect all nodes to each other
	err = ConnectNodes(nodes, ctx)
	if err != nil {
		t.Fatalf("Failed to connect nodes: %v", err)
	} else {
		log.Printf("[%s] All nodes connected successfully.", t.Name())
	}

	// Allow a moment for connections and pubsub peer discovery over the new connections
	log.Printf("[%s] Waiting briefly after manual connections...", t.Name())
	time.Sleep(5 * time.Second)
	// You could add explicit checks here using host.Network().Peers() again if needed

	// --- Content Submission (only to miner) ---
	log.Printf("[%s] Submitting initial content only to miner node %s...", t.Name(), nodes[0].Host.ID().ShortString())
	numContents := 3
	for i := 0; i < numContents; i++ {
		content := fmt.Sprintf("MinerTx-%d", i+1)
		if err := nodes[0].SubmitContent(content); err != nil {
			t.Logf("[%s] Warning: SubmitContent failed for '%s' on miner node %s: %v", t.Name(), content, nodes[0].Host.ID().ShortString(), err)
		}
	}

	// --- Wait for Mining & Broadcast ---
	totalWaitTime := 15 * time.Second
	log.Printf("[%s] Waiting %v for miner node to mine %d blocks and broadcast...", t.Name(), totalWaitTime, numContents)
	time.Sleep(totalWaitTime)

	// --- Check Convergence ---
	log.Printf("[%s] Checking for final convergence among all nodes...", t.Name())
	converged, finalTip, finalHeight := waitForConvergence(t, nodes, 10*time.Second, 2*time.Second)

	// --- Assertions (remain the same) ---
	if !converged {
		t.Errorf("[%s] Nodes failed to converge after single miner produced blocks.", t.Name())
		// ... (log final state) ...
		t.Fail()
	} else {
		if finalHeight < numContents {
			t.Errorf("[%s] Converged, but final chain height (%d) is less than submitted contents (%d).", t.Name(), finalHeight, numContents)
		} else {
			t.Logf("[%s] Test Passed: All nodes converged after single miner produced blocks. Final Tip: %s... (H:%d)", t.Name(), finalTip[:8], finalHeight)
		}
	}
}

// Test Case 2: Single Miner, Broadcast, and Convergence Verification
func TestMultiMinerBroadcastAndConvergence(t *testing.T) {
	t.Parallel()
	log.Println("--- TestMultiMinerBroadcastAndConvergence ---")
	discoveryTag := getTestDiscoveryTag(t)
	numNodes := 5
	ctx, cancel, nodes, err := setupTestNetwork(t, numNodes, discoveryTag) // setup 3 nodes
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer cancel()
	defer func() { /* ... (defer cleanup remains the same) ... */ }()

	if len(nodes) < numNodes {
		t.Fatal("Test requires at least 3 nodes.") // Sanity check
	}

	// connect all nodes to each other
	err = ConnectNodes(nodes, ctx)
	if err != nil {
		t.Fatalf("Failed to connect nodes: %v", err)
	} else {
		log.Printf("[%s] All nodes connected successfully.", t.Name())
	}

	// Allow a moment for connections and pubsub peer discovery over the new connections
	log.Printf("[%s] Waiting briefly after manual connections...", t.Name())
	time.Sleep(5 * time.Second)
	// You could add explicit checks here using host.Network().Peers() again if needed

	// --- Content Submission (only to miner) ---
	log.Printf("[%s] Submitting initial content to 3 miner node randomly", t.Name())
	// might generate forked blocks
	numContents := 7
	for i := 0; i < numContents; i++ {
		for j := 0; j < 3; j++ {
			index := rand.Intn(numNodes)
			minerNode := nodes[index]
			content := fmt.Sprintf("MinerTx-%d", i+1)
			if err := minerNode.SubmitContent(content); err != nil {
				t.Logf("[%s] Warning: SubmitContent failed for '%s' on miner node %s: %v", t.Name(), content, minerNode.Host.ID().ShortString(), err)
			}
		}
	}

	// make sure the chain would be converge at last
	numContents = 6
	for i := 0; i < numContents; i++ {
		minerNode := nodes[0]
		content := fmt.Sprintf("SingleMinerTx-%d", i+1)
		if err := minerNode.SubmitContent(content); err != nil {
			t.Logf("[%s] Warning: SubmitContent failed for '%s' on miner node %s: %v", t.Name(), content, minerNode.Host.ID().ShortString(), err)
		}
	}

	// --- Wait for Mining & Broadcast ---
	totalWaitTime := 15 * time.Second
	log.Printf("[%s] Waiting %v for miner node to mine %d blocks and broadcast...", t.Name(), totalWaitTime, numContents)
	time.Sleep(totalWaitTime)

	// --- Check Convergence ---
	log.Printf("[%s] Checking for final convergence among all nodes...", t.Name())
	converged, finalTip, finalHeight := waitForConvergence(t, nodes, 60*time.Second, 5*time.Second)

	// --- Assertions (remain the same) ---
	if !converged {
		t.Errorf("[%s] Nodes failed to converge after single miner produced blocks.", t.Name())
		// ... (log final state) ...
		t.Fail()
	} else {
		if finalHeight < numContents {
			t.Errorf("[%s] Converged, but final chain height (%d) is less than submitted contents (%d).", t.Name(), finalHeight, numContents)
		} else {
			t.Logf("[%s] Test Passed: All nodes converged after single miner produced blocks. Final Tip: %s... (H:%d)", t.Name(), finalTip[:8], finalHeight)
		}
	}
}

// Test Case 3: Invalid Block Rejection
// submits various malformed blocks directly to a node's AddBlock method
// and verifies they are rejected.
func TestInvalidBlockRejection(t *testing.T) {
	t.Parallel()
	log.Println("--- TestInvalidBlockRejection ---")
	// setup: Single node is sufficient to test AddBlock logic
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Shorter timeout OK
	defer cancel()

	// use a unique tag even for single node to avoid potential (though unlikely) interference
	discoveryTag := getTestDiscoveryTag(t)

	node, err := NewNode(ctx, 0, discoveryTag)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	// no need to Start() network components if directly calling AddBlock,
	// but starting mining loop helps establish a baseline chain quickly.
	node.Start() // Start mining etc.
	defer node.Stop()

	// submit content to node for mining
	for i := 0; i < 3; i++ {
		err = node.SubmitContent(fmt.Sprintf("Test block %d mined by node1", i))
		if err != nil {
			t.Fatalf("failed to submit content to node1: %v", err)
		}
	}

	log.Printf("[%s] Node %s started. Waiting for initial block(s)...", t.Name(), node.Host.ID().ShortString())
	// let the node mine at least one block beyond genesis
	time.Sleep(miningWaitTime) // use constant defined earlier

	node.Blockchain.mu.Lock()
	initialTip := node.Blockchain.Head
	if initialTip == nil || initialTip.Height == 0 {
		node.Blockchain.mu.Unlock()
		t.Fatalf("[%s] Node failed to mine initial block(s). Current height is 0.", t.Name())
	}
	log.Printf("[%s] Initial Tip: Height=%d, Hash=%s", t.Name(), initialTip.Height, initialTip.Block.Hash[:8])
	node.Blockchain.mu.Unlock()

	// --- Incorrect Previous Hash ---
	log.Printf("[%s] Testing rejection of block with incorrect previous hash...", t.Name())
	invalidBlockBadPrevHash := &Block{
		Index:     initialTip.Height + 10,
		Timestamp: time.Now(),
		Data:      "InvalidPreHash Block Data",
		Nonce:     12345,                            // nonce doesn't matter much here if PreHash is wrong
		PreHash:   "incorrect_previous_hash_string", // definitely wrong
		// hash will be calculated by AddBlock or should be pre-calculated based on contents
	}
	// pre-calculate hash based on its contents (assuming a CalcHash method exists)
	// invalidBlockBadPrevHash.Hash = invalidBlockBadPrevHash.CalcHash() // if needed by AddBlock

	err = node.Blockchain.AddBlock(*invalidBlockBadPrevHash)
	if err == nil {
		t.Fatalf("[%s] Accepted block with incorrect previous hash.", t.Name())
	} else {
		t.Logf("[%s] Correctly rejected block with bad previous hash: %v", t.Name(), err)
	}

	// verify tip didn't change
	node.Blockchain.mu.Lock()
	currentTip := node.Blockchain.Head
	node.Blockchain.mu.Unlock()
	if currentTip.Block.Hash != initialTip.Block.Hash {
		t.Errorf("[%s] Blockchain tip changed after rejecting block with bad previous hash. Old: %s, New: %s", t.Name(), initialTip.Block.Hash[:8], currentTip.Block.Hash[:8])
	}

	// --- Incorrect Index/Height ---
	log.Printf("[%s] Testing rejection of block with incorrect index...", t.Name())
	invalidBlockBadIndex := &Block{
		Index:     initialTip.Height + 5, // incorrect index
		Timestamp: time.Now(),
		Data:      "InvalidIndex Block Data",
		Nonce:     54321,
		PreHash:   initialTip.Block.Hash, // correct previous hash
	}

	err = node.Blockchain.AddBlock(*invalidBlockBadIndex)
	if err == nil {
		t.Fatalf("[%s] Accepted block with incorrect previous hash.", t.Name())
	} else {
		t.Logf("[%s] Correctly rejected block with bad previous hash: %v", t.Name(), err)
	}

	// Verify tip didn't change
	node.Blockchain.mu.Lock()
	currentTip = node.Blockchain.Head
	node.Blockchain.mu.Unlock()
	if currentTip.Block.Hash != initialTip.Block.Hash {
		t.Errorf("[%s] Blockchain tip changed after rejecting block with bad index. Old: %s, New: %s", t.Name(), initialTip.Block.Hash[:8], currentTip.Block.Hash[:8])
	}

	// --- Correct Hash for the Whole Block but wrong contents ---
	log.Printf("[%s] Testing rejection of block with correct hash but wrong contents...", t.Name())
	validBlockBadContents := &Block{
		Index:     initialTip.Height + 5, // incorrect index
		Timestamp: time.Now(),
		Data:      "InvalidIndex Block Data",
		// nonce will be calculated later to make this block valid
		PreHash: initialTip.Block.Hash, // correct previous hash
	}
	nonce := rand.Intn(1000000)
	validBlockBadContents.Nonce = nonce
	var hash string
	for {
		hash = validBlockBadContents.CalculateHash()
		if hash[:difficulty] == strings.Repeat("0", difficulty) {
			break
		}
		validBlockBadContents.Nonce++
	}
	validBlockBadContents.Hash = hash

	err = node.Blockchain.AddBlock(*validBlockBadContents)
	if err == nil {
		t.Fatalf("[%s] Accepted block with correct hash but wrong contents.", t.Name())
	} else {
		t.Logf("[%s] Correctly rejected block with correct hash but wrong contents: %v", t.Name(), err)
	}

	// verify tip didn't change
	node.Blockchain.mu.Lock()
	currentTip = node.Blockchain.Head
	node.Blockchain.mu.Unlock()
	if currentTip.Block.Hash != initialTip.Block.Hash {
		t.Errorf("[%s] Blockchain tip changed after rejecting block with correct hash but wrong contents. Old: %s, New: %s", t.Name(), initialTip.Block.Hash[:8], currentTip.Block.Hash[:8])
	}
}

// Test Case 4: Fork Convergence
func TestForkConvergence(t *testing.T) {
	t.Parallel()
	log.Println("--- TestForkConvergence ---")
	discoveryTag := getTestDiscoveryTag(t)
	numNodes := 20
	ctx, cancel, nodes, err := setupTestNetwork(t, numNodes, discoveryTag) // setup 3 nodes
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer cancel()
	defer func() { /* ... (defer cleanup remains the same) ... */ }()

	if len(nodes) < numNodes {
		t.Fatal("Test requires at least 3 nodes.") // Sanity check
	}

	// connect all nodes to each other
	err = ConnectNodes(nodes, ctx)
	if err != nil {
		t.Fatalf("Failed to connect nodes: %v", err)
	} else {
		log.Printf("[%s] All nodes connected successfully.", t.Name())
	}

	nodesForkSetup(nodes)

	// allow a moment for connections and pubsub peer discovery over the new connections
	log.Printf("[%s] Waiting briefly after manual connections...", t.Name())
	time.Sleep(5 * time.Second)
	// you could add explicit checks here using host.Network().Peers() again if needed

	// --- Content Submission (only to miner) ---
	log.Printf("[%s] Submitting initial content to 3 miner node randomly", t.Name())
	// generating forked blocks
	numContents := 70
	for i := 0; i < numContents; i++ {
		for j := 0; j < 3; j++ {
			index := rand.Intn(numNodes)
			minerNode := nodes[index]
			content := fmt.Sprintf("MinerTx-%d", i+1)
			if err := minerNode.SubmitContent(content); err != nil {
				t.Logf("[%s] Warning: SubmitContent failed for '%s' on miner node %s: %v", t.Name(), content, minerNode.Host.ID().ShortString(), err)
			}
		}
	}

	// no forked blocks so the chain would be converge at last
	numContents = 6
	for i := 0; i < numContents; i++ {
		minerNode := nodes[0]
		content := fmt.Sprintf("SingleMinerTx-%d", i+1)
		if err := minerNode.SubmitContent(content); err != nil {
			t.Logf("[%s] Warning: SubmitContent failed for '%s' on miner node %s: %v", t.Name(), content, minerNode.Host.ID().ShortString(), err)
		}
	}

	// --- Wait for Mining & Broadcast ---
	// estimatedBlockTime := (miningWaitTime / 2) + syncWaitTime
	// totalWaitTime := time.Duration(numContents+1) * estimatedBlockTime // Wait for numContents blocks + buffer
	// log.Printf("[%s] Waiting %v for miner node to mine %d blocks and broadcast...", t.Name(), totalWaitTime, numContents)
	time.Sleep(30 * time.Second)

	// --- Check Convergence ---
	log.Printf("[%s] Checking for final convergence among all nodes...", t.Name())
	converged, finalTip, finalHeight := waitForConvergence(t, nodes, 60*time.Second, 5*time.Second)

	// --- Assertions (remain the same) ---
	if !converged {
		t.Errorf("[%s] Nodes failed to converge after single miner produced blocks.", t.Name())
		// ... (log final state) ...
		t.Fail()
	} else {
		if finalHeight < numContents {
			t.Errorf("[%s] Converged, but final chain height (%d) is less than submitted contents (%d).", t.Name(), finalHeight, numContents)
		} else {
			t.Logf("[%s] Test Passed: All nodes converged after single miner produced blocks. Final Tip: %s... (H:%d)", t.Name(), finalTip[:8], finalHeight)
		}
	}
}

// Test case 5: Transaction de-duplication
// ensures that content submitted and included in a block is not included
// again in subsequent blocks by the same node.
func TestTransactionDeDuplication(t *testing.T) {
	t.Parallel() // mark as parallelizable if safe
	log.Println("--- TestTransactionDeDuplication ---")
	// setup: single node is sufficient to test its own mining logic for de-duplication
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	discoveryTag := getTestDiscoveryTag(t)

	node, err := NewNode(ctx, 0, discoveryTag)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	node.Start()
	defer node.Stop()

	nodeID := node.Host.ID().ShortString()
	log.Printf("[%s] Node %s started.", t.Name(), nodeID)

	// content to be submitted - make it unique for this test run
	uniqueContent := fmt.Sprintf("dedup-content-%d-%d", time.Now().UnixNano(), rand.Intn(10000))
	uniqueTimestamp := time.Now().Truncate(time.Minute).Round(0)
	log.Printf("[%s] Submitting unique content '%s' to node %s...", t.Name(), uniqueContent, nodeID)

	// submit the content for the first time
	if err := node.SubmitContentWithTimestamp(uniqueContent, uniqueTimestamp); err != nil {
		t.Fatalf("[%s] Failed to submit initial content '%s': %v", t.Name(), uniqueContent, err)
	}

	// wait for the node to mine *at least one block* hopefully containing the content
	log.Printf("[%s] Waiting %v for content '%s' to be mined...", t.Name(), miningWaitTime, uniqueContent)
	time.Sleep(miningWaitTime)

	// check if the content was mined
	node.Blockchain.mu.Lock()
	tipAfterFirstMining := node.Blockchain.Head
	heightAfterFirstMining := 0
	if tipAfterFirstMining != nil {
		heightAfterFirstMining = tipAfterFirstMining.Height
	}
	node.Blockchain.mu.Unlock()

	log.Printf("[%s] Checking if content '%s' is in blocks up to height %d...", t.Name(), uniqueContent, heightAfterFirstMining)
	// check from block 1 (genesis is 0) upwards
	foundFirstTime := findContentInChain(t, node, uniqueContent, 1, heightAfterFirstMining)

	// retry logic if not found immediately (mining might be slow)
	if !foundFirstTime && heightAfterFirstMining >= 0 { // only retry if node is up
		log.Printf("[%s] Content not found yet, waiting %v more...", t.Name(), syncWaitTime)
		time.Sleep(syncWaitTime)
		node.Blockchain.mu.Lock()
		tipAfterFirstMining = node.Blockchain.Head // re-check tip
		heightAfterFirstMining = 0
		if tipAfterFirstMining != nil {
			heightAfterFirstMining = tipAfterFirstMining.Height
		}
		node.Blockchain.mu.Unlock()
		// check again in the potentially updated height range
		foundFirstTime = findContentInChain(t, node, uniqueContent, 1, heightAfterFirstMining)
	}

	// verification of first inclusion
	if !foundFirstTime {
		t.Fatalf("[%s] FAILED: Submitted content '%s' was not found in any block up to height %d after waiting.", t.Name(), uniqueContent, heightAfterFirstMining)
	} else {
		t.Logf("[%s] OK: Verified content '%s' was included in a block at or before height %d.", t.Name(), uniqueContent, heightAfterFirstMining)
	}

	// attempt to submit duplicate content
	log.Printf("[%s] re-submitting the same content '%s' to node %s...", t.Name(), uniqueContent, nodeID)
	// it's possible SubmitContent itself prevents adding duplicates to the mempool,
	// or the mining loop ignores duplicates later. We test the end result (no duplicate in *new* blocks).
	err = node.SubmitContentWithTimestamp(uniqueContent, uniqueTimestamp)
	if err != nil {
		// this might be expected if mempool checks duplicates, log it but don't fail yet.
		t.Logf("[%s] Note: SubmitContent returned an error on duplicate submission (might be expected): %v", t.Name(), err)
	} else {
		t.Logf("[%s] Note: SubmitContent did not return an error on duplicate submission.", t.Name())
	}

	// wait for potentially more blocks to be mined
	// wait long enough for at least one, preferably several mining cycles.
	log.Printf("[%s] waiting %v for node to potentially mine more blocks after re-submission...", t.Name(), miningWaitTime+syncWaitTime)
	time.Sleep(miningWaitTime + syncWaitTime)

	// check the blocks *after* the first content was confirmed
	node.Blockchain.mu.Lock()
	tipAfterSecondMining := node.Blockchain.Head
	heightAfterSecondMining := 0
	if tipAfterSecondMining != nil {
		heightAfterSecondMining = tipAfterSecondMining.Height
	}
	node.Blockchain.mu.Unlock()

	// define the range of blocks to check for the duplicate
	startHeightForSecondCheck := heightAfterFirstMining + 1
	log.Printf("[%s] checking if duplicate content '%s' appears in NEW blocks (height %d to %d)...", t.Name(), uniqueContent, startHeightForSecondCheck, heightAfterSecondMining)

	// final verification
	if startHeightForSecondCheck > heightAfterSecondMining {
		// no new blocks were mined after the re-submission attempt.
		t.Logf("[%s] OK: No new blocks were mined (height %d). De-duplication implicitly holds for new blocks.", t.Name(), heightAfterSecondMining)
	} else {
		// search only in the newly mined blocks
		foundSecondTime := findContentInChain(t, node, uniqueContent, startHeightForSecondCheck, heightAfterSecondMining)

		// assertion
		if foundSecondTime {
			// this is the failure condition!
			t.Errorf("[%s] FAILED: Duplicate content '%s' WAS FOUND in a new block between height %d and %d after being re-submitted.", t.Name(), uniqueContent, startHeightForSecondCheck, heightAfterSecondMining)
		} else {
			// this is the success condition!
			t.Logf("[%s] PASS: Duplicate content '%s' was NOT found in subsequent blocks (checked range %d-%d). De-duplication successful.", t.Name(), uniqueContent, startHeightForSecondCheck, heightAfterSecondMining)
		}
	}
}

// Test case 6: Crash recovery
func TestNodeCrashRecovery(t *testing.T) {
	specMiningWaitTime := 25 * time.Second
	// create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// create two nodes
	log.Printf("[%s] creating node1 (will stay online)...", t.Name())
	node1, err := NewNode(ctx, 0, "test-recovery")
	if err != nil {
		t.Fatalf("failed to create node1: %v", err)
	}
	node1.Start()
	defer node1.Stop()

	// get node1's ID for logging
	node1ID := node1.Host.ID().ShortString()
	node1Addrs := node1.Host.Addrs()
	log.Printf("[%s] node1 (%s) started successfully at %v", t.Name(), node1ID, node1Addrs)

	// submit content to node1 for mining
	for i := 0; i < 3; i++ {
		err = node1.SubmitContent(fmt.Sprintf("Test block %d mined by node1", i))
		if err != nil {
			t.Fatalf("failed to submit content to node1: %v", err)
		}
	}

	// wait for node1 to mine blocks
	log.Printf("[%s] waiting for node1 to mine blocks...", t.Name())
	time.Sleep(specMiningWaitTime)

	// check node1's blockchain height
	node1.Blockchain.mu.Lock()
	node1Height := node1.Blockchain.Head.Height
	node1Hash := node1.Blockchain.Head.Block.Hash
	node1.Blockchain.mu.Unlock()

	log.Printf("[%s] node1 (%s) blockchain height: %d, tip: %s",
		t.Name(), node1ID, node1Height, node1Hash[:8])

	if node1Height < 2 {
		t.Fatalf("expected node1 to mine multiple blocks, but height is only %d", node1Height)
	}

	// now create node2 (simulating a crashed node that is rejoining)
	log.Printf("[%s] creating node2 (simulating crashed node rejoining)...", t.Name())
	node2, err := NewNode(ctx, 0, "test-recovery")
	if err != nil {
		t.Fatalf("failed to create node2: %v", err)
	}
	defer node2.Stop()

	// get initial state of node2
	node2.Blockchain.mu.Lock()
	initialNode2Height := node2.Blockchain.Head.Height
	node2.Blockchain.mu.Unlock()

	node2ID := node2.Host.ID().ShortString()
	log.Printf("[%s] node2 (%s) initial height: %d (genesis block)", t.Name(), node2ID, initialNode2Height)

	// Define a custom start function instead of trying to override the Start method
	customStart := func() {
		// start the PubSub message handler
		go node2.pubsubHandler()

		// start Discovery processes
		dutil.Advertise(node2.Ctx, node2.Discovery, node2.DiscoveryTag)
		log.Printf("Node %s advertising with tag %s\n", node2.Host.ID().ShortString(), node2.DiscoveryTag)
		go node2.discoverPeers()

		// start the mining loop
		node2.miningLoopWait.Add(1)
		go node2.miningLoop()

		log.Printf("Node %s started successfully (without auto-recovery).", node2.Host.ID().ShortString())
	}

	// Call our custom start instead of the normal start
	customStart()

	// manually connect node2 to node1
	node1Info := peer.AddrInfo{
		ID:    node1.Host.ID(),
		Addrs: node1.Host.Addrs(),
	}

	log.Printf("[%s] manually connecting node2 to node1...", t.Name())
	err = node2.Host.Connect(ctx, node1Info)
	if err != nil {
		t.Fatalf("failed to connect node2 to node1: %v", err)
	}

	// verify connection was established
	time.Sleep(2 * time.Second)
	if len(node2.Host.Network().Peers()) == 0 {
		t.Fatalf("node2 failed to connect to node1")
	}
	log.Printf("[%s] node2 successfully connected to node1", t.Name())

	// manually trigger recovery on node2
	log.Printf("[%s] manually triggering recovery on node2...", t.Name())
	node2.StartRecovery()

	// check if node2 recovered properly
	node2.Blockchain.mu.Lock()
	node2Height := node2.Blockchain.Head.Height
	node2Hash := node2.Blockchain.Head.Block.Hash
	node2.Blockchain.mu.Unlock()

	log.Printf("[%s] after recovery: node1 height=%d, tip=%s; node2 height=%d, tip=%s",
		t.Name(), node1Height, node1Hash[:8], node2Height, node2Hash[:8])

	// verify node2 has caught up to node1
	if node2Height != node1Height {
		t.Errorf("recovery failed: node2 height (%d) != node1 height (%d)",
			node2Height, node1Height)
	}

	if node2Hash != node1Hash {
		t.Errorf("recovery failed: blockchain tips don't match. node1: %s, node2: %s",
			node1Hash[:8], node2Hash[:8])
	}

	log.Printf("[%s] recovery test successful: node2 caught up to node1", t.Name())
}
