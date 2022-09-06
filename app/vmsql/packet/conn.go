package packet

import (
	"io"
	"net"
)

type VmsqlConn struct {
	net.Conn
	data   []byte
	closed bool
	read   int
}

func NewVmsqlConn() *VmsqlConn {
	return &VmsqlConn{}
}

// Read implements the net.Conn interface.
func (vc *VmsqlConn) Read(b []byte) (n int, err error) {
	if len(vc.data) == 0 {
		err = io.EOF
		return
	}
	n = copy(b, vc.data)
	vc.read += n
	vc.data = vc.data[n:]
	return
}

// Write implements the net.Conn interface.
func (vc *VmsqlConn) Write(b []byte) (n int, err error) {
	vc.data = append(vc.data, b...)
	return len(b), nil
}

// Close implements the net.Conn interface.
func (vc *VmsqlConn) Close() error {
	vc.closed = true
	return nil
}

func (vc *VmsqlConn) Data() []byte {
	return vc.data
}
