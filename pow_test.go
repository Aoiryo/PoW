package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
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
	testTimeout          = 90 * time.Second // Increased timeout for potentially complex scenarios
	setupWaitTime        = 10 * time.Second // Time for initial node startup and basic discovery
	syncWaitTime         = 20 * time.Second // Increased time for sync/propagation after events
	miningWaitTime       = 25 * time.Second // Increased time to allow for mining cycles
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
		// if len(nodes) == 1 {
		// 	nodes[0].Blockchain.mu.Lock()
		// 	defer nodes[0].Blockchain.mu.Unlock()
		// 	if len(nodes[0].Blockchain.Chain) > 0 {
		// 		tip := nodes[0].Blockchain.Chain[len(nodes[0].Blockchain.Chain)-1]
		// 		return true, tip.Hash, tip.Header.Height
		// 	}
		// }
		return true, "", -1 // 0 or 1 node, guaranteed convergence
	}

	// check if all nodes have same head block
	for i := 1; i < len(nodes); i++ {
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
		node.Blockchain.mu.Lock()
		headList[i] = node.Blockchain.Head
		node.Blockchain.mu.Unlock()
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
	// firstNode := nodes[0]
	// firstNode.Blockchain.mu.RLock()
	// if len(firstNode.Blockchain.Chain) == 0 {
	// 	firstNode.Blockchain.mu.RUnlock()
	// 	t.Logf("[%s] Convergence check warning: Node 0 has empty chain.", t.Name())
	// 	return false, "", -1 // cannot converge on empty chain if others exist
	// }
	// tip := firstNode.Blockchain.Chain[len(firstNode.Blockchain.Chain)-1]
	// firstTipHash := tip.Hash
	// firstTipHeight := tip.Header.Height
	// firstNode.Blockchain.mu.RUnlock()

	// for i := 1; i < len(nodes); i++ {
	// 	currentNode := nodes[i]
	// 	currentNode.Blockchain.mu.RLock()
	// 	if len(currentNode.Blockchain.Chain) == 0 {
	// 		currentNode.Blockchain.mu.RUnlock()
	// 		t.Logf("[%s] Convergence check failed: Node %d has empty chain, Node 0 tip %s (H:%d)", t.Name(), i, firstTipHash[:8], firstTipHeight)
	// 		return false, firstTipHash, firstTipHeight
	// 	}
	// 	currentTipBlock := currentNode.Blockchain.Chain[len(currentNode.Blockchain.Chain)-1]
	// 	currentTipHash := currentTipBlock.Hash
	// 	currentTipHeight := currentTipBlock.Header.Height
	// 	currentNode.Blockchain.mu.RUnlock()

	// 	if currentTipHash != firstTipHash || currentTipHeight != firstTipHeight {
	// 		t.Logf("[%s] Convergence check failed: Node 0 tip %s (H:%d), Node %d tip %s (H:%d)", t.Name(), firstTipHash[:8], firstTipHeight, i, currentTipHash[:8], currentTipHeight)
	// 		return false, firstTipHash, firstTipHeight // Return first node's state for reference
	// 	}
	// }
	// t.Logf("[%s] Convergence check passed. All %d nodes at tip %s... (H:%d)", t.Name(), len(nodes), firstTipHash[:8], firstTipHeight)
	// return true, firstTipHash, firstTipHeight
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

// --- Test Cases ---

// PASSED
// Test Case: Single Miner, Broadcast, and Convergence Verification
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
	// minerNode := nodes[0]
	// listenerNode1 := nodes[1]
	// listenerNode2 := nodes[2]

	// // --- Manually Connect Nodes ---
	// // Ensure miner can reach listeners and listeners can reach miner (or at least one way)
	// // It's often sufficient to connect listeners TO the miner.
	// log.Printf("[%s] Manually connecting listener nodes to miner node...", t.Name())
	// minerAddrInfo := peer.AddrInfo{
	// 	ID:    minerNode.Host.ID(),
	// 	Addrs: minerNode.Host.Addrs(),
	// }

	// // Connect Listener 1 to Miner
	// log.Printf("[%s] Connecting Node 1 (%s) to Miner (%s)...", t.Name(), listenerNode1.Host.ID().ShortString(), minerNode.Host.ID().ShortString())
	// if err := listenerNode1.Host.Connect(ctx, minerAddrInfo); err != nil {
	// 	// Make connection failure fatal for this test as it relies on connectivity
	// 	t.Fatalf("[%s] Failed to manually connect Node 1 to Miner: %v", t.Name(), err)
	// } else {
	// 	log.Printf("[%s] Node 1 successfully initiated connection to Miner.", t.Name())
	// }

	// // Connect Listener 2 to Miner
	// log.Printf("[%s] Connecting Node 2 (%s) to Miner (%s)...", t.Name(), listenerNode2.Host.ID().ShortString(), minerNode.Host.ID().ShortString())
	// if err := listenerNode2.Host.Connect(ctx, minerAddrInfo); err != nil {
	// 	t.Fatalf("[%s] Failed to manually connect Node 2 to Miner: %v", t.Name(), err)
	// } else {
	// 	log.Printf("[%s] Node 2 successfully initiated connection to Miner.", t.Name())
	// }

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
	estimatedBlockTime := (miningWaitTime / 2) + syncWaitTime
	totalWaitTime := time.Duration(numContents+1) * estimatedBlockTime // Wait for numContents blocks + buffer
	log.Printf("[%s] Waiting %v for miner node to mine %d blocks and broadcast...", t.Name(), totalWaitTime, numContents)
	time.Sleep(totalWaitTime)

	// --- Check Convergence ---
	log.Printf("[%s] Checking for final convergence among all nodes...", t.Name())
	converged, finalTip, finalHeight := waitForConvergence(t, nodes, 5*time.Second, 1*time.Second)

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

// // Test Case: Single Miner, Broadcast, and Convergence Verification
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

	// minerNode := nodes[0]
	// listenerNode1 := nodes[1]
	// listenerNode2 := nodes[2]

	// // --- Manually Connect Nodes ---
	// // Ensure miner can reach listeners and listeners can reach miner (or at least one way)
	// // It's often sufficient to connect listeners TO the miner.
	// log.Printf("[%s] Manually connecting listener nodes to miner node...", t.Name())
	// minerAddrInfo := peer.AddrInfo{
	// 	ID:    minerNode.Host.ID(),
	// 	Addrs: minerNode.Host.Addrs(),
	// }

	// // Connect Listener 1 to Miner
	// log.Printf("[%s] Connecting Node 1 (%s) to Miner (%s)...", t.Name(), listenerNode1.Host.ID().ShortString(), minerNode.Host.ID().ShortString())
	// if err := listenerNode1.Host.Connect(ctx, minerAddrInfo); err != nil {
	// 	// Make connection failure fatal for this test as it relies on connectivity
	// 	t.Fatalf("[%s] Failed to manually connect Node 1 to Miner: %v", t.Name(), err)
	// } else {
	// 	log.Printf("[%s] Node 1 successfully initiated connection to Miner.", t.Name())
	// }

	// // Connect Listener 2 to Miner
	// log.Printf("[%s] Connecting Node 2 (%s) to Miner (%s)...", t.Name(), listenerNode2.Host.ID().ShortString(), minerNode.Host.ID().ShortString())
	// if err := listenerNode2.Host.Connect(ctx, minerAddrInfo); err != nil {
	// 	t.Fatalf("[%s] Failed to manually connect Node 2 to Miner: %v", t.Name(), err)
	// } else {
	// 	log.Printf("[%s] Node 2 successfully initiated connection to Miner.", t.Name())
	// }

	// Allow a moment for connections and pubsub peer discovery over the new connections
	log.Printf("[%s] Waiting briefly after manual connections...", t.Name())
	time.Sleep(5 * time.Second)
	// You could add explicit checks here using host.Network().Peers() again if needed

	// --- Content Submission (only to miner) ---
	log.Printf("[%s] Submitting initial content to 3 miner node randomly", t.Name())
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

	// --- Wait for Mining & Broadcast ---
	estimatedBlockTime := (miningWaitTime / 2) + syncWaitTime
	totalWaitTime := time.Duration(numContents+1) * estimatedBlockTime // Wait for numContents blocks + buffer
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
