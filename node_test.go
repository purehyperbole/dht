package dht

import (
	"crypto/rand"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUDPSend(t *testing.T) {
	r1, err := net.ResolveUDPAddr("udp", "127.0.0.1:8001")
	require.Nil(t, err)

	s1, err := net.ListenUDP("udp", r1)
	require.Nil(t, err)

	r2, err := net.ResolveUDPAddr("udp", "127.0.0.1:8002")
	require.Nil(t, err)

	_, err = net.ListenUDP("udp", r2)
	require.Nil(t, err)

	b := make([]byte, 2048)
	rand.Read(b)

	start := time.Now()

	for i := 0; i < 1000000; i++ {
		wb, err := s1.WriteToUDP(b, r2)
		if err != nil {
			fmt.Println(wb)
			panic(err)
		}
		if wb < 2048 {
			fmt.Println("incomplete")
		}
	}

	fmt.Println(time.Since(start))
}
