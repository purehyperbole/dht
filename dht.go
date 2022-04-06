package dht

import (
	"context"
	"errors"
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

	err = d.listen()
	if err != nil {
		return nil, err
	}

	if cfg.BootstrapAddress != "" {

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

		d.listeners = append(d.listeners, newListener(c.(*net.UDPConn), d.routing, d.cache, d.storage, d.config.Timeout))
	}

	return nil
}

// Store a value on the network. If the value fails to store, the provided callback will be returned with the error
func (d *DHT) Store(key, value []byte, ttl time.Duration, callback func(err error)) {
	// get the k closest nodes to store the value to
	nodes := d.routing.closestN(key, K)

	// track the number of successful stores we've had from each node
	// before calling the user provided callback
	var r int32

	// get a spare buffer to generate our requests with
	buf := d.pool.Get().(*flatbuffers.Builder)
	defer d.pool.Put(buf)

	for _, n := range nodes {
		// generate a new random request ID
		rid := randomID()

		buf.Reset()

		// create the store table
		k := buf.CreateByteVector(key)
		v := buf.CreateByteVector(value)

		protocol.StoreStart(buf)
		protocol.StoreAddKey(buf, k)
		protocol.StoreAddValue(buf, v)
		s := protocol.StoreEnd(buf)

		// build the event to send
		eid := buf.CreateByteVector(rid)
		snd := buf.CreateByteVector(d.config.LocalID)

		protocol.EventStart(buf)
		protocol.EventAddId(buf, eid)
		protocol.EventAddSender(buf, snd)
		protocol.EventAddEvent(buf, protocol.EventTypeSTORE)
		protocol.EventAddResponse(buf, false)
		protocol.EventAddPayload(buf, s)

		e := protocol.EventEnd(buf)

		buf.Finish(e)

		// select the next listener to send our request
		err := d.listeners[(atomic.AddInt32(&d.cl, 1)-1)%int32(len(d.listeners))].request(
			n.address,
			rid,
			buf.FinishedBytes(),
			func(event *protocol.Event, err error) {
				// TODO : we call the user provided callback as soon as there's an error
				// ideally, we should consider the store a success if a minimum number of
				// nodes successfully managed to store the value
				if err != nil {
					callback(err)
					return
				}

				if atomic.AddInt32(&r, 1) == int32(len(nodes)) {
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
