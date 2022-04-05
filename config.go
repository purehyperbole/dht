package dht

import "time"

// Config configuration parameters for the dht
type Config struct {
	// LocalID the id of this node. If not specified, a random id will be generated
	LocalID []byte
	// ListenAddress the udp ip and port to listen on
	ListenAddress string
	// BootstrapAddress the udp ip and port of the bootstrap node
	BootstrapAddress string
	// Listeners the number of threads that will listen on the designated udp port
	Listeners int
	// Timeout the amount of time before a peer is declared unresponsive and removed
	Timeout time.Duration
}
