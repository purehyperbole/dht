package dht

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

const (
	// PacketHeaderSize the size of the header we use to reconstruct data
	PacketHeaderSize = KEY_BYTES + 2

	// MaxEventSize the maximum size of an event packet size
	MaxEventSize = 65024

	// MaxPacketSize the size of packets we will send according to MTU,
	// minus a 8 bytes for the UDP header
	MaxPacketSize = 1472

	// MaxPayloadSize the maximum payload of our packet. The max packet size,
	// minus 22 bytes for our fragment header
	MaxPayloadSize = MaxPacketSize - PacketHeaderSize
)

// pool for building and reassembling udp packets
type packetManager struct {
	fragments sync.Map
	pool      sync.Pool
	hasher    maphash.Hash
	mu        sync.Mutex
}

func newPacketManager() *packetManager {
	m := &packetManager{
		pool: sync.Pool{
			New: func() any {
				return &packet{
					buf: make([]byte, 1<<17-1),
				}
			},
		},
	}

	m.hasher.SetSeed(maphash.MakeSeed())

	return m
}

// marks a packet as done and returns it to the pool
func (m *packetManager) done(p *packet) {
	m.pool.Put(p)
}

// returns true if an events data doesn't fit and needs fragmenting
func (m *packetManager) needsFragmenting(data []byte) bool {
	return (len(data)/MaxPacketSize-1)+2 > 1
}

// takes an events data and fragments it into packets that fit inside of MTU
func (m *packetManager) fragment(id, data []byte) *packet {
	p := m.pool.Get().(*packet)

	p.frg = (len(data)/MaxPacketSize - 1) + 2
	p.len = int32(len(data) + (p.frg * PacketHeaderSize))
	p.pos = 0

	for i := 0; i < p.frg; i++ {
		offset := i * (MaxPacketSize)

		// write the header to the fragment
		copy(p.buf[offset:], id)
		p.buf[offset+KEY_BYTES] = byte(i + 1)
		p.buf[offset+KEY_BYTES+1] = byte(p.frg)

		// write the data of the packet
		copy(p.buf[offset+PacketHeaderSize:offset+PacketHeaderSize+MaxPayloadSize], data[i*MaxPayloadSize:]) // (i+1)*MaxPayloadSize]
	}

	return p
}

// assembles a packet into an event. if there are missing fragments, this will return nil
func (m *packetManager) assemble(f []byte) *packet {
	// shortcut this is the event isn't fragmented
	if f[KEY_BYTES+2] == 1 {
		return &packet{
			len: int32(len(f) - PacketHeaderSize),
			buf: f[PacketHeaderSize:],
		}
	}

	// TODO : avoid locking here
	m.mu.Lock()

	m.hasher.Reset()
	m.hasher.Write(f[:KEY_BYTES])
	k := m.hasher.Sum64()

	m.mu.Unlock()

	var p *packet

	// load the packet from our packet cache
	// or create it if its a new fragmented packet
	// we've not seen before
	pi, ok := m.fragments.Load(k)
	if !ok {
		p = m.pool.Get().(*packet)
		p.frg = int(f[PacketHeaderSize-1])

		// this may be racy if two listeners
		// receive different fragments at the same
		// time and the cache does not contain a packet
		// for them!!
		pl, ok := m.fragments.LoadOrStore(k, p)
		if ok {
			// return our unused packet to the pool
			m.pool.Put(p)

			p = pl.(*packet)
		}
	} else {
		p = pi.(*packet)
	}

	// add the fragment to the packet. if it's complete, return the packet
	if p.add(f) {
		m.fragments.Delete(k)
		return p
	}

	return nil
}

/*
	we need to fragment events into smaller chunks if they do not fit into an
	IP packet. we split a single event into mutliple packets and assemble
	them at the other end.

	Each "packet" will have an additional 22 byte header that allows the
	receiving end to determine which fragmented part belongs to what UDP packet:

	| 20 bytes | byte | byte  |
	| event id | part | total |
*/
type packet struct {
	// 128kb buffer used to construct packet fragments
	buf []byte
	// the number of remaining fragments left in the packet
	frg int
	// the position in the buffer we currently are
	pos int
	// the length of the data in the buffer
	len int32
}

// returns the next fragment to transmit. if there's none left to send, it returns nil
func (p *packet) next() []byte {
	if p.pos >= int(p.len) {
		return nil
	}

	ps := MaxPacketSize

	// caculate the size of this packet
	if p.pos+ps > int(p.len) {
		ps = int(p.len) - p.pos
	}

	p.pos = p.pos + ps

	return p.buf[p.pos-ps : p.pos]
}

// adds copies the fragments data to the packet buffer
// returns true if all of the fragments are present
func (p *packet) add(f []byte) bool {
	offset := int(f[KEY_BYTES]) - 1
	data := f[PacketHeaderSize:]

	copy(p.buf[MaxPayloadSize*offset:], data)

	return atomic.AddInt32(&p.len, int32(len(data))) > (int32(p.frg)-1)*MaxPayloadSize
}

// data returns the full data in the packets buffer
func (p *packet) data() []byte {
	return p.buf[:p.len]
}

// returns true if we have a completed set of fragments
// we could have used a set with different fragments, but
// as we can work out if a packet is complete if the size
// of the buffer is bigger than the size of a fragments payload
// multiplied by the number of fragments
func (p *packet) complete() bool {
	return atomic.LoadInt32(&p.len) > (int32(p.frg)-1)*MaxPayloadSize
}
