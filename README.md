# P2P Blockchain with Proof of Work

A simple blockchain implementation using libp2p for peer-to-peer communication and Proof of Work (PoW) for consensus.

### Contributors

| **Chuqi Zhang**                  | **Likeer Xu**                  |
| :------------------------:       | :----------------------:       |
| Andrew ID: `chuqiz`              | Andrew ID: `xlikeer`           |
| GitHub: `Chuqi-Leo-Zhang`        | GitHub: `Aoiryo`               |

### GitHub Classroom Team

`pluto`

## Files Overview

Under the folder `PoW`, we have the following main files:

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

## Core Features

- Peer-to-peer networking using libp2p
- GossipSub for efficient block propagation
- Proof of Work consensus algorithm
- Fork resolution using longest chain rule
- Distributed node discovery

## Design Overview

Our blockchain system implements a fully decentralized peer-to-peer network with Proof of Work (PoW) consensus. Here's how the key components work together:

### Data Structures
- **Block**: Contains the fundamental blockchain data including index, timestamp, content data, previous hash, nonce, and current block hash
- **BlockNode**: Wrapper structure that enables tree-like storage of the blockchain with parent-child relationships
- **Blockchain**: Maintains the blockchain state with a map of all blocks, active tips, and current head

### Node Communication
- **libp2p Network**: Nodes communicate via libp2p, a modular P2P networking stack
- **Kademlia DHT**: Used for peer discovery and distributed routing (this is not utilized in our test cases, because discovery takes time and is not stable enough. We manually connect nodes to each other instead. )
- **GossipSub**: Efficient block propagation using a gossip-based publish-subscribe model. Nodes subscribe to the same topic and publish/broadcast blocks to the same topic.

### Mining Process
1. Transactions are collected in a node's mempool
2. Mining loop checks for pending transactions
3. When transactions exist, the node attempts to find a valid nonce by incrementing and hashing
4. A block is considered valid when its hash begins with a specified number of zeros (difficulty level)
5. Successfully mined blocks are added to the local blockchain and broadcast to all peers
6. A fail buffer is used to resolve out-of-order packet problem and handle the sequence of blocks received correctly

### Consensus & Fork Resolution
- **Proof of Work**: Nodes compete to find valid nonces, requiring computational work. For example, if the difficulty level is 2, the node needs to find a nonce such that the hash of the whole block begins with two zeros.
- **Longest Chain Rule**: When forks occur, nodes follow the chain with the highest cumulative difficulty. In our implementation, we store a tree structure of the blockchain, so that we can merge any delayed blocks into the tree. Eventually, the tree will converge to the longest chain. The check is done by comparing the head node of the blockchain. If the head node is the same, the chain is the same recursively.
- **Block Validation**: Each block is verified for proper hash, index, and parent relationship. 
- **Chain Pruning**: Maintains efficiency by removing abandoned forks after sufficient confirmation depth. 

### Network Resilience
- **Recovery Protocol**: Nodes joining the network sync with peers to obtain the latest blockchain state. This is implemented using stream handlers. New nodes can set up a stream with an existing node and send a message to request the latest blockchain state. Longest chain will be considered as the correct chain, and the node will update its blockchain to the longest chain using streaming as well. 
- **Common Ancestor Finding**: Algorithm for efficient chain synchronization from fork points. This is the helper function for synchronizing two chains from different nodes.
- **Duplicate Detection**: Prevents re-processing of already seen blocks. This is implemented by checking the transaction string and the timestamp of the block. If the block has been seen, it will be discarded.
- **Fail Buffer**: A temporary storage mechanism implemented in the pubsubHandler that addresses the out-of-order block reception problem in P2P networks. When blocks are received via gossip but cannot be immediately added to the blockchain (typically because their parent blocks haven't arrived yet), they are stored in this buffer rather than being discarded. The system periodically attempts to reprocess blocks from the fail buffer whenever a new block is successfully added to the chain, ensuring that blocks arriving out of sequence are eventually incorporated into the blockchain once their dependencies are satisfied. This mechanism significantly improves chain synchronization efficiency across network partitions and during high network latency scenarios.

### Peer Communication Protocols (implemented using libp2p and stream handlers)
- **Block Broadcasting**: Newly mined blocks are broadcast to all peers. 
- **Chain Info Exchange**: Nodes can query peers for blockchain height and tip information. 
- **Headers Synchronization**: Efficient block header sharing for chain comparison. 
- **Block Retrieval**: On-demand fetching of full blocks during recovery. 

This design ensures fault tolerance and allows the network to reach consensus even in the presence of malicious or faulty nodes, as long as the majority of the network's hash power is controlled by honest participants.

## Test Instructions

### How to use
0. Clone the repository
1. Simply run `make all` to build and run the tests
### Test Cases 

- Test 0: Basic communication between nodes (10 pts)
  - Verifies that nodes can successfully communicate using the libp2p PubSub system
  - Tests that messages published on a topic by one node are received by subscribed nodes

- Test 1: Single Miner, Broadcast, and Convergence Verification (15 pts)
  - Checks that a single miner can produce blocks containing submitted content
  - Verifies that mined blocks are properly broadcast to all nodes
  - Ensures all nodes converge to the same blockchain state

- Test 2: Multiple Miners, Broadcast, and Convergence Verification (30 pts)
  - Tests system behavior with multiple nodes mining simultaneously
  - Creates potential fork situations by having random nodes mine blocks
  - Verifies that network eventually converges to a single longest chain

- Test 3: Invalid Block Rejection (25 pts)
  - Tests that nodes correctly reject invalid blocks with:
    - Incorrect previous hash values
    - Invalid block index values
    - Correctly formatted hashes but wrong contents

- Test 4: Fork Convergence (50 pts)
  - Creates an explicit fork scenario with conflicting blocks
  - Tests the system's ability to handle multiple competing chains
  - Verifies that all nodes eventually converge on the longest valid chain

- Test 5: Transaction de-duplication (35 pts)
  - Verifies that blocks don't contain duplicate transactions
  - Tests that resubmitted content with the same timestamp is properly detected and ignored
  - Ensures the deduplication mechanism works correctly in the mining process

- Test 6: Crash recovery (35 pts)
  - Simulates a node crashing and rejoining the network by late joining
  - Tests the recovery protocol's ability to synchronize with the current blockchain state
  - Verifies that recovered nodes properly catch up with the network's consensus

## Documentation

Please refer to ./PoW/pow-doc.txt for `go doc` style documentation.
