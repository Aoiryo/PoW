if [ ! -f go.mod ]; then
    go mod init lab4
fi 

go get github.com/libp2p/go-libp2p \
       github.com/libp2p/go-libp2p-kad-dht \
       github.com/libp2p/go-libp2p-pubsub