package main

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// simple test, check the message publishing and subscribing functionality between nodes
func TestPubSubCommunication(t *testing.T) {
	// create 2 nodes
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// create two nodes, using fixed ports to ensure address predictability
	node1, err := NewNode(ctx, 10001, "test-pubsub-tag")
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}
	node1.Start()
	defer node1.Stop()
	log.Printf("Node 1 created with ID: %s, addresses: %v", node1.Host.ID().ShortString(), node1.Host.Addrs())

	node2, err := NewNode(ctx, 10002, "test-pubsub-tag")
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}
	node2.Start()
	defer node2.Stop()
	log.Printf("Node 2 created with ID: %s, addresses: %v", node2.Host.ID().ShortString(), node2.Host.Addrs())

	// manually connect two nodes
	addrInfo1 := peer.AddrInfo{
		ID:    node1.Host.ID(),
		Addrs: node1.Host.Addrs(),
	}
	log.Printf("Attempting to manually connect node2 to node1 (ID: %s)", node1.Host.ID().ShortString())
	if err := node2.Host.Connect(ctx, addrInfo1); err != nil {
		t.Logf("Warning: Failed to manually connect node2 to node1: %v", err)
	} else {
		log.Printf("Node 2 successfully connected to Node 1")
	}

	// wait for connection to stabilize
	log.Println("Waiting for connection to stabilize...")
	time.Sleep(5 * time.Second)

	// create a custom message receiving channel
	messageChan := make(chan string)
	var wg sync.WaitGroup
	wg.Add(1)

	// create a custom topic subscription on node2
	testTopic, err := node2.PubSub.Join("test-message-topic")
	if err != nil {
		t.Fatalf("Failed to join test topic: %v", err)
	}

	subscription, err := testTopic.Subscribe()
	if err != nil {
		t.Fatalf("Failed to subscribe to test topic: %v", err)
	}

	// try to receive message
	go func() {
		defer wg.Done()
		msg, err := subscription.Next(ctx)
		if err != nil {
			log.Printf("Error receiving message: %v", err)
			return
		}
		log.Printf("Node 2 received message from %s: %s", msg.ReceivedFrom.ShortString(), string(msg.Data))
		messageChan <- string(msg.Data)
	}()

	// wait for a while to ensure the subscription is established
	time.Sleep(2 * time.Second)

	// node1 also joins the same topic and publishes a message
	node1Topic, err := node1.PubSub.Join("test-message-topic")
	if err != nil {
		t.Fatalf("Failed to join test topic on node1: %v", err)
	}

	// check the connected peers
	log.Printf("Node 1 connected peers: %d", len(node1.Host.Network().Peers()))
	log.Printf("Node 2 connected peers: %d", len(node2.Host.Network().Peers()))

	testMessage := "Hello from Node 1!"
	log.Printf("Node 1 publishing message: %s", testMessage)
	err = node1Topic.Publish(ctx, []byte(testMessage))
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// wait for the message to be received within a timeout
	select {
	case receivedMsg := <-messageChan:
		if receivedMsg != testMessage {
			t.Errorf("Expected message '%s', but got '%s'", testMessage, receivedMsg)
		} else {
			t.Logf("Test passed: Message successfully received!")
		}
	case <-time.After(20 * time.Second):
		t.Errorf("Timeout: No message received within 20 seconds")
	}

	// wait for the receiving goroutine to complete
	wg.Wait()
}
