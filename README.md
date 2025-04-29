# P2P Blockchain with Proof of Work

A simple blockchain implementation using libp2p for peer-to-peer communication and Proof of Work (PoW) for consensus.

## Files Overview

- **pow.go**: Core implementation of the blockchain system including:
  - Data structures for Block and Blockchain
  - Node implementation for P2P networking using libp2p
  - Mining algorithm with Proof of Work
  - Block validation and fork resolution
  - PubSub-based block propagation

- **pow_test.go**: Test suite for the blockchain functionality:
  - Tests for basic blockchain operations
  - Network partition tests
  - Fork resolution tests
  - Crash recovery tests

- **pubsub_test.go**: Tests focused on PubSub communication:
  - Verification of message publishing and subscribing between nodes
  - Testing of direct P2P connections

## Getting Started (not sure)

0. Clone the repository
1. Run `sh setup_env.sh`
2. Run `go mod download` to fetch dependencies
3. Run tests like `go test -run TestSingleMinerBroadcastAndConvergence`

## Core Features

- Peer-to-peer networking using libp2p
- GossipSub for efficient block propagation
- Proof of Work consensus algorithm
- Fork resolution using longest chain rule
- Distributed node discovery
