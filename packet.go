package dht

import (
	"encoding/binary"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// PacketHeaderSize the size of the header we use to reconstruct data
	PacketHeaderSize = KEY_BYTES + 4

	// MaxEventSize the maximum size of an event packet size
	MaxEventSize = 65024

	// MaxPacketSize the size of packets we will send according to MTU,
	// minus a 8 bytes for the UDP header
	MaxPacketSize = 1472

	// MaxPayloadSize the maximum payload of our packet. The max packet size,
	// minus 24 bytes for our fragment header
	MaxPayloadSize = MaxPacketSize - PacketHeaderSize
)

// pool for building and reassembling udp packets
type packetManager struct {
	packets sync.Map
	pool    sync.Pool
	hasher  maphash.Hash
	mu      sync.Mutex
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

	go m.cleanup()

	return m
}

// marks a packet as done and returns it to the pool
func (m *packetManager) done(p *packet) {
	if len(p.buf) < 1<<17-1 {
		return
	}

	m.pool.Put(p)
}

// returns true if an events data doesn't fit and needs fragmenting
func (m *packetManager) needsFragmenting(data []byte) bool {
	return (len(data)/MaxPacketSize-1)+2 > 1
}

// takes an events data and fragments it into packets that fit inside of MTU
func (m *packetManager) fragment(id, data []byte) *packet {
	p := m.pool.Get().(*packet)

	p.frg = (len(data)/MaxPayloadSize - 1) + 2
	if len(data)%MaxPayloadSize == 0 {
		p.frg--
	}

	p.len = len(data) + (p.frg * PacketHeaderSize)
	p.pos = 0

	var i, offset int

	for {
		if offset >= p.len {
			break
		}

		// write the header to the fragment
		copy(p.buf[offset:], id)
		p.buf[offset+KEY_BYTES] = byte(i + 1)
		p.buf[offset+KEY_BYTES+1] = byte(p.frg)
		binary.LittleEndian.PutUint16(p.buf[offset+KEY_BYTES+2:], uint16(len(data)))

		copy(p.buf[offset+PacketHeaderSize:offset+MaxPacketSize], data[i*MaxPayloadSize:])

		offset = offset + MaxPacketSize
		i++
	}

	return p
}

// assembles a packet into an event. if there are missing fragments, this will return nil
func (m *packetManager) assemble(f []byte) *packet {
	// shortcut this if the event isn't fragmented
	if f[KEY_BYTES+1] == 1 {
		return &packet{
			len: len(f) - PacketHeaderSize,
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
	pi, ok := m.packets.Load(k)
	if !ok {
		p = m.pool.Get().(*packet)
		p.frg = int(f[KEY_BYTES+1])
		p.len = int(binary.LittleEndian.Uint16(f[KEY_BYTES+2:]))
		p.pos = 0
		p.ttl = time.Now().Add(time.Second * 5)

		// this may be racy if two listeners
		// receive different fragments at the same
		// time and the cache does not contain a packet
		// for them!!
		pl, ok := m.packets.LoadOrStore(k, p)
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
		m.packets.Delete(k)
		return p
	}

	return nil
}

func (m *packetManager) cleanup() {
	for {
		time.Sleep(time.Second * 5)
		now := time.Now()

		m.packets.Range(func(key, value any) bool {
			p := value.(*packet)
			if !p.complete() && now.After(p.ttl) {
				// remove packets that
				m.packets.Delete(key)
			}
			return true
		})
	}
}

/*
We need to fragment events into smaller chunks if they do not fit into an
IP packet.

Each fragment will have an additional 24 byte header that allows the
receiving end to determine which fragmented part belongs to what UDP packet:

| 20 bytes | byte | byte  | 2 bytes |
| event id | part | total | size    |
*/
type packet struct {
	// 128kb buffer used to construct packet fragments
	buf []byte
	// the number of remaining fragments left in the packet
	frg int
	// the length of the data in the buffer
	len int
	// the position in the buffer we currently are
	pos int32
	// the time this packet expires if not completed
	ttl time.Time
}

// returns the next fragment to transmit. if there's none left to send, it returns nil
func (p *packet) next() []byte {
	if int(p.pos) >= p.len {
		return nil
	}

	ps := MaxPacketSize

	// caculate the size of this packet
	if int(p.pos)+ps > int(p.len) {
		ps = p.len - int(p.pos)
	}

	p.pos = p.pos + int32(ps)

	return p.buf[int(p.pos)-ps : p.pos]
}

// adds copies the fragments data to the packet buffer
// returns true if all of the fragments are present
func (p *packet) add(f []byte) bool {
	offset := int(f[KEY_BYTES]) - 1
	data := f[PacketHeaderSize:]

	copy(p.buf[MaxPayloadSize*offset:], data)

	return atomic.AddInt32(&p.pos, int32(len(data))) == int32(p.len)
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
	return atomic.LoadInt32(&p.pos) == int32(p.len)
}
