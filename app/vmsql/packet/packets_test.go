package packet

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPacketsNext(t *testing.T) {
	conn := NewVmsqlConn()
	defer conn.Close()

	packets := NewPackets(conn)
	data := []byte{0x01, 0x02, 0x03}

	{
		// header
		buff := common.NewBuffer(64)
		buff.WriteU24(3)
		buff.WriteU8(0)
		buff.WriteBytes(data)

		conn.Write(buff.Data())
		body, err := packets.Next()
		assert.Nil(t, err)
		assert.Equal(t, body, data)
	}

	{
		// header
		buff := common.NewBuffer(64)
		buff.WriteU24(3)
		buff.WriteU8(1)
		buff.WriteBytes(data)

		conn.Write(buff.Data())
		body, err := packets.Next()
		assert.Nil(t, err)
		assert.Equal(t, body, data)
	}

	// seq error test
	{
		// header
		buff := common.NewBuffer(64)
		buff.WriteU24(3)
		buff.WriteU8(1)
		buff.WriteBytes(data)

		conn.Write(buff.Data())
		_, err := packets.Next()
		want := "pkt.read.seq[1]!=pkt.actual.seq[2] (errno 1835) (sqlstate HY000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// reset seq
	{
		assert.Equal(t, packets.seq, uint8(2))
		packets.ResetSeq()
		assert.Equal(t, packets.seq, uint8(0))
	}
}

func TestPacketsNextFail(t *testing.T) {
	conn := NewVmsqlConn()
	defer conn.Close()

	packets := NewPackets(conn)
	data1 := []byte{0x00, 0x00, 0x00}
	data2 := []byte{0x00, 0x00, 0x00, 0x00}
	data3 := []byte{0x01, 0x10, 0x00, 0x00}

	{
		conn.Write(data1)
		_, err := packets.Next()
		assert.NotNil(t, err)
	}

	{
		conn.Write(data2)
		_, err := packets.Next()
		assert.Nil(t, err)
	}

	{
		conn.Write(data3)
		_, err := packets.Next()
		assert.NotNil(t, err)
	}
}

func TestPacketsWrite(t *testing.T) {
	conn := NewVmsqlConn()
	defer conn.Close()

	buff := common.NewBuffer(64)
	packets := NewPackets(conn)
	data := []byte{0x01, 0x02, 0x03}

	{
		buff.WriteU24(3)
		buff.WriteU8(0)
		buff.WriteBytes(data)
		want := buff.Data()

		err := packets.Write(data)
		assert.Nil(t, err)
		got := conn.Data()
		assert.Equal(t, want, got)
	}

	{
		buff.WriteU24(3)
		buff.WriteU8(1)
		buff.WriteBytes(data)
		want := buff.Data()

		err := packets.Write(data)
		assert.Nil(t, err)
		got := conn.Data()
		assert.Equal(t, want, got)
	}
}

func TestPacketsWriteCommand(t *testing.T) {
	conn := NewVmsqlConn()
	defer conn.Close()

	buff := common.NewBuffer(64)
	packets := NewPackets(conn)
	cmd := 0x03
	data := []byte{0x01, 0x02, 0x03}

	{
		buff.WriteU24(3 + 1)
		buff.WriteU8(0)
		buff.WriteU8(uint8(cmd))
		buff.WriteBytes(data)
		want := buff.Data()

		err := packets.WriteCommand(byte(cmd), data)
		assert.Nil(t, err)
		got := conn.Data()
		assert.Equal(t, want, got)
	}
}

func TestPacketsWriteOK(t *testing.T) {
	conn := NewVmsqlConn()
	defer conn.Close()

	wPackets := NewPackets(conn)
	err := wPackets.WriteOK(1, 1, 1, 1)
	assert.Nil(t, err)

	conn.Data()
}

func TestPacketsWriteError(t *testing.T) {
	conn := NewVmsqlConn()
	defer conn.Close()

	wPackets := NewPackets(conn)
	err := wPackets.WriteERR(1, "YH000", "err:%v", "unknow")
	assert.Nil(t, err)
}

func TestPacketsEOF(t *testing.T) {
	conn := NewVmsqlConn()
	defer conn.Close()

	wPackets := NewPackets(conn)
	rPackets := NewPackets(conn)
	// EOF
	{
		err := wPackets.AppendEOF(1, 1)
		assert.Nil(t, err)
		wPackets.Flush()

		err = rPackets.ReadEOF()
		assert.Nil(t, err)
	}

	// OK with EOF header.
	{
		err := wPackets.AppendOKWithEOFHeader(1, 1, 1, 1)
		assert.Nil(t, err)
		wPackets.Flush()

		err = rPackets.ReadEOF()
		assert.Nil(t, err)
	}
}
