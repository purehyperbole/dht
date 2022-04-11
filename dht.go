package dht

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
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
	// storage for values that saved to this node
	storage Storage
	// routing table that stores routing information about the network
	routing *routingTable
	// cache that tracks requests sent to other nodes
	cache *cache
	// manages fragmented packets that are larger than MTU
	packet *packetManager
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

	if int(cfg.Timeout) == 0 {
		cfg.Timeout = time.Minute
	}

	if cfg.Listeners < 1 {
		cfg.Listeners = runtime.GOMAXPROCS(0)
	}

	if cfg.Storage == nil {
		cfg.Storage = newInMemoryStorage()
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
		storage: cfg.Storage,
		packet:  newPacketManager(),
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

	br := make(chan error, len(cfg.BootstrapAddresses))
	bn := make([]*node, len(cfg.BootstrapAddresses))

	for i := range cfg.BootstrapAddresses {
		addr, err := net.ResolveUDPAddr("udp", cfg.BootstrapAddresses[i])
		if err != nil {
			return nil, err
		}

		bn[i] = &node{address: addr}
	}

	fmt.Println("FIND NODE")
	// TODO : this should be a recursive lookup, use journey
	d.findNodes(bn, cfg.LocalID, func(err error) {
		br <- err
	})

	var successes int

	for range cfg.BootstrapAddresses {
		err := <-br
		if err != nil {
			log.Printf("bootstrap failed: %s\n", err.Error())
			continue
		}
		successes++
	}

	fmt.Println("DONE")

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

		d.listeners = append(d.listeners, newListener(c.(*net.UDPConn), d.config.LocalID, d.routing, d.cache, d.storage, d.packet, d.config.Timeout))
	}

	return nil
}

// Store a value on the network. If the value fails to store, the provided callback will be returned with the error
func (d *DHT) Store(key, value []byte, ttl time.Duration, callback func(err error)) {
	if len(key) != KEY_BYTES {
		callback(errors.New("key must be 20 bytes in length"))
		return
	}

	// value must be smaller than 32 kb
	if len(value) > 32768 {
		callback(errors.New("value must be less than 32kb in length"))
		return
	}

	v := []*Value{
		{
			Key:   key,
			Value: value,
			TTL:   ttl,
		},
	}

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
			d.storage.Set(key, value, ttl)

			if len(ns) == 1 {
				// we're the only node, so call the callback immediately
				callback(nil)
				return
			}

			continue
		}

		// generate a new random request ID and event
		rid := randomID()
		req := eventStoreRequest(buf, rid, d.config.LocalID, v)

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
	if len(key) != KEY_BYTES {
		callback(nil, errors.New("key must be 20 bytes in length"))
		return
	}

	// we should check our own cache first before sending a request
	v, ok := d.storage.Get(key)
	if ok {
		callback(v.Value, nil)
		return
	}

	// a correct implementation should send mutiple requests concurrently,
	// but here we're only send a request to the closest node
	ns := d.routing.closestN(key, K)
	if len(ns) == 0 {
		callback(nil, errors.New("no nodes found!"))
		return
	}

	// K iterations to find the key we want
	j := newJourney(d.config.LocalID, key, K)
	j.add(ns)

	// try lookup to best 3 nodes
	for _, n := range j.next(3) {
		// get a spare buffer to generate our requests with
		buf := d.pool.Get().(*flatbuffers.Builder)
		defer d.pool.Put(buf)

		// generate a new random request ID
		rid := randomID()
		req := eventFindValueRequest(buf, rid, d.config.LocalID, key)

		// select the next listener to send our request
		err := d.listeners[(atomic.AddInt32(&d.cl, 1)-1)%int32(len(d.listeners))].request(
			n.address,
			rid,
			req,
			d.findValueCallback(n.id, key, callback, j),
		)

		if err != nil {
			// if we fail to write to the socket, send the error to the callback immediately
			callback(nil, err)
			return
		}
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
func (d *DHT) findValueCallback(id, key []byte, callback func(value []byte, err error), j *journey) func(event *protocol.Event, err error) {
	return func(event *protocol.Event, err error) {
		if err != nil {
			if errors.Is(err, ErrRequestTimeout) {
				d.routing.remove(id)
			}
		}

		journeyCompleted, shouldError := j.responseReceived()

		if journeyCompleted {
			// ignore this response, we've already received what we've needed
			return
		}

		// journey is completed, ignore this response
		if err != nil {
			// if there's an actual error, send that to the user
			if shouldError {
				callback(nil, err)
			}
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
		if f.ValueLength() > 0 {
			if j.finish(true) {
				callback(f.ValueBytes(), nil)
			}
			return
		} else if f.NodesLength() < 1 {
			// mark the journey as finished so no more
			// requests will be made
			if j.finish(false) {
				callback(nil, errors.New("value not found"))
			}
			return
		}

		// collect the new nodes from the response
		newNodes := make([]*node, f.NodesLength())

		for i := 0; i < f.NodesLength(); i++ {
			nd := new(protocol.Node)

			if !f.Nodes(nd, i) {
				callback(nil, errors.New("bad find value node data"))
				return
			}

			nad := &net.UDPAddr{
				IP:   make(net.IP, 4),
				Port: int(binary.LittleEndian.Uint16(nd.AddressBytes()[4:])),
			}

			copy(nad.IP, nd.AddressBytes()[:4])

			nid := make([]byte, KEY_BYTES)
			copy(nid, nd.IdBytes())

			newNodes[i] = &node{
				id:      id,
				address: nad,
			}
		}

		// add them to the journey and then get the next recommended routes to query
		j.add(newNodes)

		ns := j.next(3)
		if ns == nil {
			if j.finish(false) {
				callback(nil, errors.New("value not found"))
			}
			return
		}

		// the key wasn't found, so send a request to the next node
		// get a spare buffer to generate our requests with
		buf := d.pool.Get().(*flatbuffers.Builder)
		defer d.pool.Put(buf)

		for _, n := range ns {
			// generate a new random request ID
			rid := randomID()
			req := eventFindValueRequest(buf, rid, d.config.LocalID, key)

			// select the next listener to send our request
			err = d.listeners[(atomic.AddInt32(&d.cl, 1)-1)%int32(len(d.listeners))].request(
				n.address,
				rid,
				req,
				d.findValueCallback(n.id, key, callback, j),
			)

			if err != nil {
				// if we fail to write to the socket, send the error to the callback immediately
				if j.finish(false) {
					callback(nil, err)
					return
				}
			}
		}
	}
}

func (d *DHT) findNodes(ns []*node, target []byte, callback func(err error)) {
	// create the journey here, but don't add the bootstrap
	// node as we don't know it's id yet
	j := newJourney(d.config.LocalID, target, K)

	// get a spare buffer to generate our requests with
	buf := d.pool.Get().(*flatbuffers.Builder)
	defer d.pool.Put(buf)

	for _, n := range ns {
		// generate a new random request ID and event
		rid := randomID()
		req := eventFindNodeRequest(buf, rid, d.config.LocalID, target)

		fmt.Println("FIND NODE -- ", hex.EncodeToString(rid))
		// select the next listener to send our request
		err := d.listeners[(atomic.AddInt32(&d.cl, 1)-1)%int32(len(d.listeners))].request(
			n.address,
			rid,
			req,
			d.findNodeCallback(target, callback, j),
		)

		if err != nil {
			// if we fail to write to the socket, send the error to the callback immediately
			callback(err)
			return
		}
	}
}

func (d *DHT) findNodeCallback(target []byte, callback func(err error), j *journey) func(*protocol.Event, error) {
	return func(event *protocol.Event, err error) {
		_, shouldError := j.responseReceived()

		// journey is completed, ignore this response
		if err != nil {
			// if there's an actual error, send that to the user
			if shouldError {
				callback(err)
			}
			return
		}

		payloadTable := new(flatbuffers.Table)

		if !event.Payload(payloadTable) {
			callback(errors.New("invalid response to find node request"))
			return
		}

		f := new(protocol.FindNode)
		f.Init(payloadTable.Bytes, payloadTable.Pos)

		newNodes := make([]*node, f.NodesLength())

		for i := 0; i < f.NodesLength(); i++ {
			fn := new(protocol.Node)

			if f.Nodes(fn, i) {
				nad := &net.UDPAddr{
					IP:   make(net.IP, 4),
					Port: int(binary.LittleEndian.Uint16(fn.AddressBytes()[4:])),
				}

				copy(nad.IP, fn.AddressBytes()[:4])

				// create a copy of the node id
				nid := make([]byte, fn.IdLength())
				copy(nid, fn.IdBytes())

				d.routing.insert(nid, nad)

				newNodes[i] = &node{
					id:      nid,
					address: nad,
				}
			}
		}

		j.add(newNodes)

		ns := j.next(3)
		if ns == nil {
			// we've completed our search of nodes
			if j.finish(false) {
				callback(nil)
			}
			return
		}

		buf := d.pool.Get().(*flatbuffers.Builder)
		defer d.pool.Put(buf)

		for _, n := range ns {
			// generate a new random request ID and event
			rid := randomID()
			req := eventFindNodeRequest(buf, rid, d.config.LocalID, target)

			fmt.Println("FIND NODE CB -- ", hex.EncodeToString(rid))

			// select the next listener to send our request
			err := d.listeners[(atomic.AddInt32(&d.cl, 1)-1)%int32(len(d.listeners))].request(
				n.address,
				rid,
				req,
				d.findNodeCallback(target, callback, j),
			)

			if err != nil {
				// if we fail to write to the socket, send the error to the callback immediately
				callback(err)
				return
			}
		}
	}
}

// monitors peers on the network and sends them ping requests
func (d *DHT) monitor() {
	for {
		time.Sleep(time.Hour / 2)

		now := time.Now()

		var nodes []*node

		for i := 0; i < KEY_BITS; i++ {
			d.routing.buckets[i].iterate(func(n *node) {
				// if we haven't seen this node in a while,
				// add it to list of nodes to ping
				if n.seen.Add(time.Hour / 2).After(now) {
					nodes = append(nodes, n)
				}
			})
		}

		buf := d.pool.Get().(*flatbuffers.Builder)

		for _, n := range nodes {
			// send a ping to each node to see if it's still alive
			rid := randomID()
			req := eventPing(buf, rid, d.config.LocalID)

			err := d.listeners[(atomic.AddInt32(&d.cl, 1)-1)%int32(len(d.listeners))].request(
				n.address,
				rid,
				req,
				func(event *protocol.Event, err error) {
					if err != nil {
						if errors.Is(err, ErrRequestTimeout) {
							d.routing.remove(n.id)
						} else {
							log.Println(err)
						}
					} else {
						d.routing.seen(n.id)
					}
				},
			)

			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
			}
		}

		d.pool.Put(buf)
	}
}

// Key creates a new 20 byte key hasehed with sha1 from a string, byte slice or int
func Key(k any) []byte {
	var h [20]byte
	switch key := k.(type) {
	case string:
		h = sha1.Sum([]byte(key))
	case []byte:
		h = sha1.Sum(key)
	case int:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(key))
		h = sha1.Sum(b)
	default:
		panic("unsupported key type!")
	}

	return h[:]
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
