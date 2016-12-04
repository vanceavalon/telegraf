package client

import (
	"fmt"
	"io"
	"net"
	"net/url"
)

const (
	// UDPPayloadSize is a reasonable default payload size for UDP packets that
	// could be travelling over the internet.
	UDPPayloadSize = 512
)

// UDPConfig is the config data needed to create a UDP Client
type UDPConfig struct {
	// URL should be of the form "udp://host:port"
	// or "udp://[ipv6-host%zone]:port".
	URL string

	// PayloadSize is the maximum size of a UDP client message, optional
	// Tune this based on your network. Defaults to UDPPayloadSize.
	PayloadSize int
}

func NewUDP(config UDPConfig) (Client, error) {
	p, err := url.Parse(config.URL)
	if err != nil {
		return nil, fmt.Errorf("Error parsing UDP url [%s]: %s", config.URL, err)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", p.Host)
	if err != nil {
		return nil, fmt.Errorf("Error resolving UDP Address [%s]: %s", p.Host, err)
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, fmt.Errorf("Error dialing UDP address [%s]: %s",
			udpAddr.String(), err)
	}

	size := config.PayloadSize
	if size == 0 {
		size = UDPPayloadSize
	}
	buf := make([]byte, size)
	return &udpClient{conn: conn, buffer: buf}, nil
}

type udpClient struct {
	conn   *net.UDPConn
	buffer []byte
}

func (c *udpClient) Query(command string) error {
	return nil
}

func (c *udpClient) Write(b []byte) (int, error) {
	return c.conn.Write(b)
}

// write params are ignored by the UDP client
func (c *udpClient) WriteWithParams(b []byte, wp WriteParams) (int, error) {
	return c.Write(b)
}

// size is ignored by the UDP client.
func (c *udpClient) WriteStream(r io.Reader, size int) (int, error) {
	n, err := io.CopyBuffer(c.conn, r, c.buffer)
	if int(n) != size {
		return int(n), fmt.Errorf("Expected to write %d bytes, only wrote %d", size, n)
	}
	return int(n), err
}

// size is ignored by the UDP client.
// write params are ignored by the UDP client
func (c *udpClient) WriteStreamWithParams(r io.Reader, size int, wp WriteParams) (int, error) {
	return c.WriteStream(r, size)
}

func (c *udpClient) Close() error {
	return c.conn.Close()
}
