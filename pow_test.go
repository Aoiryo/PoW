package main

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	err = node.SubmitContent("Test content for mining")
	if err != nil {
		t.Fatalf("Failed to submit content: %v", err)
	}

	// Wait for mining
	time.Sleep(15 * time.Second)

	// check that the blockchain has grown
	node.Blockchain.mu.Lock()
	chainLen := node.Blockchain.Head.Height
	node.Blockchain.mu.Unlock()

	if chainLen == 0 {
		t.Errorf("Expected chain to grow beyond genesis, but length is %d", chainLen)
	} else {
		t.Logf("Chain has grown to length %d", chainLen)
	}
}

const (
	// Using a dynamic tag per test run to minimize interference if tests run quasi-parallel locally
	testDiscoveryTagBase = "blockchain-test-network"
	testTimeout          = 500 * time.Second // Increased timeout for potentially complex scenarios
	setupWaitTime        = 120 * time.Second // Time for initial node startup and basic discovery
	syncWaitTime         = 120 * time.Second // Increased time for sync/propagation after events
	miningWaitTime       = 20 * time.Second  // Increased time to allow for mining cycles
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

// --- Test Cases ---

// Test case 6: Crash recovery
func TestNodeCrashRecovery(t *testing.T) {
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
	time.Sleep(miningWaitTime)

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
