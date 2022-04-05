package dht

// DHT represents the distributed hash table
type DHT struct {
	routing *routingTable
}

// New creates a new dht
func New(cfg *Config) *DHT {

	return &DHT{
		routing: newRoutingTable(),
	}
}
