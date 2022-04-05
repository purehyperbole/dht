package dht

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"syscall"

	"github.com/purehyperbole/dht/protocol"
	"golang.org/x/sys/unix"
)

// DHT represents the distributed hash table
type DHT struct {
	config  *Config
	routing *routingTable
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
	}

	for i := 0; i < cfg.Listeners; i++ {
		err = d.listen()
		if err != nil {
			return nil, err
		}
	}

	if cfg.BootstrapAddress != "" {

	}

	return d, nil
}

func (d *DHT) listen() error {
	cfg := net.ListenConfig{
		Control: control,
	}

	c, err := cfg.ListenPacket(context.Background(), "udp", d.config.ListenAddress)
	if err != nil {
		return err
	}

	go d.process(c.(*net.UDPConn))

	return nil
}

func (d *DHT) process(c *net.UDPConn) {
	// buffer maximum udp payload
	b := make([]byte, 65527)

	for {
		rb, addr, err := c.ReadFromUDP(b)
		if err != nil {
			panic(err)
		}

		fmt.Println("received from:", addr, "size:", rb)

		e := protocol.GetRootAsEvent(b[:rb], 0)

		switch e.Event() {
		case protocol.EventTypePING:
			e.
		case protocol.EventTypePONG:
		default:
		case protocol.EventTypeSTORE:
		case protocol.EventTypeFIND_NODE:
		case protocol.EventTypeFIND_VALUE:
		}
	}
}

// borrow this from github.com/libp2p/go-reusepor as we don't care about other operating systems right now :)
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
