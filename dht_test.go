package dht

import (
	"crypto/rand"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDHT(t *testing.T) {
	c := &Config{
		ListenAddress: "127.0.0.1:9000",
	}

	_, err := New(c)
	require.Nil(t, err)

	addr, err := net.ResolveUDPAddr("udp", c.ListenAddress)
	require.Nil(t, err)

	conn, err := net.DialUDP("udp", nil, addr)
	require.Nil(t, err)

	data := make([]byte, 64000)
	rand.Read(data)

	wb, err := conn.Write(data)
	//wb, err := conn.Write([]byte("hello there"))
	fmt.Println(err)
	require.Nil(t, err)

	fmt.Println("wrote", wb, "bytes")

	time.Sleep(time.Second)
}
