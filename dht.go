package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/purehyperbole/dht/protocol"
	"golang.org/x/sys/unix"
)

// DHT represents the distributed hash table
type DHT struct {
	// config used for the dht
	config *Config
	// routing table that stores routing information about the network
	routing *routingTable
	// cache that tracks requests sent to other nodes
	cache *cache
	// storage for values that saved to this node
	storage *storage
	// udp listeners that are handling requests to/from other nodes
	listeners []*listener
	// pool of flatbuffer builder bufs to use when sending requests
	pool sync.Pool
	// the current listener to use when sending data
	cl int32
}

// New creates a new dht
func New(cfg *Config) (*DHT, error) {
	if cfg.LocalID == nil {
		cfg.LocalID = randomID()
	} else if len(cfg.LocalID) != KEY_BYTES {
		return nil, errors.New("node id length is incorrect")
	}

	if cfg.Timeout.Nanoseconds() == 0 {
		cfg.Timeout = time.Minute
	}

	if cfg.Listeners < 1 {
		cfg.Listeners = runtime.GOMAXPROCS(0)
	}

	addr, err := net.ResolveUDPAddr("udp", cfg.ListenAddress)
	if err != nil {
		return nil, err
	}

	n := &node{
		id:      cfg.LocalID,
		address: addr,
	}

	d := &DHT{
		config:  cfg,
		routing: newRoutingTable(n),
		cache:   newCache(cfg.Timeout),
		storage: newStorage(),
		pool: sync.Pool{
			New: func() any {
				return flatbuffers.NewBuilder(1024)
			},
		},
	}

	// start the udp listeners
	err = d.listen()
	if err != nil {
		return nil, err
	}

	// add the local node to our own routing table
	d.routing.insert(n.id, addr)

	br := make(chan error, 3)

	for i := range cfg.BootstrapAddresses {
		addr, err := net.ResolveUDPAddr("udp", cfg.BootstrapAddresses[i])
		if err != nil {
			return nil, err
		}

		d.findNodes(&node{address: addr}, cfg.LocalID, func(err error) {
			br <- err
		})
	}

	var successes int

	for range cfg.BootstrapAddresses {
		err := <-br
		if err != nil {
			log.Printf("bootstrap failed: %s\n", err.Error())
			continue
		}
		successes++
	}

	if successes < 1 && len(cfg.BootstrapAddresses) > 1 {
		return nil, errors.New("bootstrapping failed")
	}

	return d, nil
}

func (d *DHT) listen() error {
	for i := 0; i < d.config.Listeners; i++ {
		cfg := net.ListenConfig{
			Control: control,
		}

		// start one of several listeners
		c, err := cfg.ListenPacket(context.Background(), "udp", d.config.ListenAddress)
		if err != nil {
			return err
		}

		d.listeners = append(d.listeners, newListener(c.(*net.UDPConn), d.config.LocalID, d.routing, d.cache, d.storage, d.config.Timeout))
	}

	return nil
}

// Store a value on the network. If the value fails to store, the provided callback will be returned with the error
func (d *DHT) Store(key, value []byte, ttl time.Duration, callback func(err error)) {
	// get the k closest nodes to store the value to
	ns := d.routing.closestN(key, K)

	if len(ns) < 1 {
		callback(errors.New("no nodes found!"))
		return
	}

	// track the number of successful stores we've had from each node
	// before calling the user provided callback
	var r int32

	// get a spare buffer to generate our requests with
	buf := d.pool.Get().(*flatbuffers.Builder)
	defer d.pool.Put(buf)

	for _, n := range ns {
		// shortcut the request if its to the local node
		if bytes.Equal(n.id, d.config.LocalID) {
			d.storage.set(key, value, time.Now().Add(ttl))

			if len(ns) == 1 {
				// we're the only node, so call the callback immediately
				callback(nil)
				return
			}

			continue
		}

		// generate a new random request ID and event
		rid := randomID()
		req := eventStoreRequest(buf, rid, d.config.LocalID, key, value)

		// select the next listener to send our request
		err := d.listeners[(atomic.AddInt32(&d.cl, 1)-1)%int32(len(d.listeners))].request(
			n.address,
			rid,
			req,
			func(event *protocol.Event, err error) {
				// TODO : we call the user provided callback as soon as there's an error
				// ideally, we should consider the store a success if a minimum number of
				// nodes successfully managed to store the value
				if err != nil {
					callback(err)
					return
				}

				if atomic.AddInt32(&r, 1) == int32(len(ns)) {
					// we've had the correct number of responses back, so lets call the
					// user provided callback with a success
					callback(nil)
				}
			},
		)

		if err != nil {
			// if we fail to write to the socket, send the error to the callback immediately
			callback(err)
			return
		}
	}
}

// Find finds a value on the network if it exists. Any returned value will not be safe to
// use outside of the callback, so you should copy it if its needed elsewhere
func (d *DHT) Find(key []byte, callback func(value []byte, err error)) {
	// a correct implementation should send mutiple requests concurrently,
	// but here we're only send a request to the closest node
	n := d.routing.closest(key)
	if n == nil {
		callback(nil, errors.New("no nodes found!"))
		return
	}

	// TODO : we should check our own cache first before sending a request?
	// shortcut the request if its to the local node
	if bytes.Equal(n.id, d.config.LocalID) {
		value, ok := d.storage.get(key)
		if !ok {
			callback(value, errors.New("key not found"))
		} else {
			callback(value, nil)
		}
		return
	}

	// get a spare buffer to generate our requests with
	buf := d.pool.Get().(*flatbuffers.Builder)
	defer d.pool.Put(buf)

	// track the number of recursive lookups we've made
	var r int

	// generate a new random request ID
	rid := randomID()
	req := eventFindValueRequest(buf, rid, d.config.LocalID, key)

	// select the next listener to send our request
	err := d.listeners[(atomic.AddInt32(&d.cl, 1)-1)%int32(len(d.listeners))].request(
		n.address,
		rid,
		req,
		d.findValueCallback(key, callback, r+1),
	)

	if err != nil {
		// if we fail to write to the socket, send the error to the callback immediately
		callback(nil, err)
		return
	}
}

// Close shuts down the dht
func (d *DHT) Close() error {
	for i := 0; i < len(d.listeners); i++ {
		err := d.listeners[i].conn.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// TODO : this is all pretty garbage, refactor!
// return the callback used to handle responses to our findValue requests, tracking the number of requests we have made
func (d *DHT) findValueCallback(key []byte, callback func(value []byte, err error), requests int) func(event *protocol.Event, err error) {
	return func(event *protocol.Event, err error) {
		// TODO : we call the user provided callback as soon as there's an error
		// ideally, we should consider the store a success if a minimum number of
		// nodes successfully managed to store the value
		if err != nil {
			// TODO : if this is a timeout, we should try the next closest node
			callback(nil, err)
			return
		}

		payloadTable := new(flatbuffers.Table)

		if !event.Payload(payloadTable) {
			callback(nil, errors.New("invalid response to find value request"))
			return
		}

		f := new(protocol.FindValue)
		f.Init(payloadTable.Bytes, payloadTable.Pos)

		// check if we received the value or if we received a list of closest
		// neighbours that might have the key
		if f.NodesLength() < 1 {
			if f.ValueLength() == 0 {
				// no value or closer node was found, so the key does not exist?
				// TODO : check if this is the right thing to do. a new node may
				// not yet have the key
				callback(nil, errors.New("value not found"))
			} else {
				callback(f.ValueBytes(), nil)
			}
			return
		}

		// TODO : this should be k, but that's an excessive amount of requests to make
		// so we half that value
		if requests >= K/2 {
			callback(nil, errors.New("value not found"))
			return
		}

		// get the first returned node so we can query it next
		nd := new(protocol.Node)

		if !f.Nodes(nd, 0) {
			callback(nil, errors.New("bad find value node data"))
			return
		}

		address, err := net.ResolveUDPAddr("udp", string(nd.AddressBytes()))
		if err != nil {
			callback(nil, errors.New("find value response contains a node with an invalid udp address"))
			return
		}

		// the key wasn't found, so send a request to the next node
		// get a spare buffer to generate our requests with
		buf := d.pool.Get().(*flatbuffers.Builder)
		defer d.pool.Put(buf)

		// TODO : we should also track if we're sending to the same node more than once!
		// track the number of recursive lookups we've made
		var r int

		// generate a new random request ID
		rid := randomID()
		req := eventFindValueRequest(buf, rid, d.config.LocalID, key)

		// select the next listener to send our request
		err = d.listeners[(atomic.AddInt32(&d.cl, 1)-1)%int32(len(d.listeners))].request(
			address,
			rid,
			req,
			d.findValueCallback(key, callback, r+1),
		)

		if err != nil {
			// if we fail to write to the socket, send the error to the callback immediately
			callback(nil, err)
			return
		}
	}
}

func (d *DHT) findNodes(n *node, target []byte, callback func(err error)) {
	// get a spare buffer to generate our requests with
	buf := d.pool.Get().(*flatbuffers.Builder)
	defer d.pool.Put(buf)

	// generate a new random request ID and event
	rid := randomID()
	req := eventFindNodeRequest(buf, rid, d.config.LocalID, target)

	// select the next listener to send our request
	err := d.listeners[(atomic.AddInt32(&d.cl, 1)-1)%int32(len(d.listeners))].request(
		n.address,
		rid,
		req,
		func(event *protocol.Event, err error) {
			if err != nil {
				callback(err)
				return
			}

			payloadTable := new(flatbuffers.Table)

			if !event.Payload(payloadTable) {
				callback(errors.New("invalid response to find node request"))
				return
			}

			f := new(protocol.FindNode)
			f.Init(payloadTable.Bytes, payloadTable.Pos)

			for i := 0; i < f.NodesLength(); i++ {
				fn := new(protocol.Node)

				if f.Nodes(fn, i) {
					nad, err := net.ResolveUDPAddr("udp", string(fn.AddressBytes()))
					if err != nil {
						callback(fmt.Errorf("find node response includes an invalid node address: %w", err))
						return
					}

					// create a copy of the node id
					nid := make([]byte, fn.IdLength())
					copy(nid, fn.IdBytes())

					d.routing.insert(nid, nad)
				}
			}

			callback(nil)
		},
	)

	if err != nil {
		// if we fail to write to the socket, send the error to the callback immediately
		callback(err)
		return
	}
}

// "borrow" this from github.com/libp2p/go-reuseport as we don't care about other operating systems right now :)
func control(network, address string, c syscall.RawConn) error {
	var err error

	c.Control(func(fd uintptr) {
		err = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
		if err != nil {
			return
		}

		err = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		if err != nil {
			return
		}
	})

	return err
}
