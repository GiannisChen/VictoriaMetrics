package packet

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/protocol"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/sqlerror"
	"net"
)

const (
	// PACKET_MAX_SIZE used for the max packet size.
	PACKET_MAX_SIZE = 1<<24 - 1 // (16MB - 1）
)

// Packet presents the packet tuple.
type Packet struct {
	SequenceID byte
	Data       []byte
}

// Packets presents the stream tuple.
type Packets struct {
	seq    uint8
	stream *Stream
}

// NewPackets creates the new packets.
func NewPackets(c net.Conn) *Packets {
	return &Packets{
		stream: NewStream(c, PACKET_MAX_SIZE),
	}
}

// Append appends packets to buffer but not write to stream.
// This is underlying packet unit.
// NOTICE: SequenceID++
func (p *Packets) Append(rawdata []byte) error {
	pkt := common.NewBuffer(64)

	// body length(24bits):
	// payload length
	pkt.WriteU24(uint32(len(rawdata)))

	// SequenceID
	pkt.WriteU8(p.seq)

	// body
	pkt.WriteBytes(rawdata)
	if err := p.stream.Append(pkt.Data()); err != nil {
		return err
	}
	p.seq++
	return nil
}

// Next used to read the next packet.
func (p *Packets) Next() ([]byte, error) {
	pkt, err := p.stream.Read()
	if err != nil {
		return nil, err
	}

	if pkt.SequenceID != p.seq {
		return nil, sqlerror.NewSQLErrorf(sqlerror.ER_MALFORMED_PACKET, "pkt.read.seq[%v]!=pkt.actual.seq[%v]", pkt.SequenceID, p.seq)
	}
	p.seq++
	return pkt.Data, nil
}

// Write writes the packet to the wire.
// It packed as:
// [header]
// [payload]
func (p *Packets) Write(payload []byte) error {
	payLen := len(payload)
	pkt := common.NewBuffer(64)

	// body length(24bits)
	pkt.WriteU24(uint32(payLen))

	// SequenceID
	pkt.WriteU8(p.seq)

	// body
	pkt.WriteBytes(payload)
	if err := p.stream.Write(pkt.Data()); err != nil {
		return err
	}
	p.seq++
	return nil
}

// WriteCommand writes a command packet to the wire.
func (p *Packets) WriteCommand(command byte, payload []byte) error {
	// reset packet sequence
	p.seq = 0
	pkt := common.NewBuffer(64)

	// body length(24bits):
	// command length + payload length
	payLen := len(payload)
	pkt.WriteU24(uint32(1 + payLen))

	// SequenceID
	pkt.WriteU8(p.seq)

	// command
	pkt.WriteU8(command)

	// body
	pkt.WriteBytes(payload)
	if err := p.stream.Write(pkt.Data()); err != nil {
		return err
	}
	p.seq++
	return nil
}

// ParseERR used to parse the ERR packet.
func (p *Packets) ParseERR(data []byte) error {
	return protocol.UnPackERR(data)
}

// WriteERR writes ERR packet to the wire.
func (p *Packets) WriteERR(errorCode uint16, sqlState string, format string, args ...interface{}) error {
	e := &protocol.ERR{
		ErrorCode:    errorCode,
		SQLState:     sqlState,
		ErrorMessage: fmt.Sprintf(format, args...),
	}
	return p.Write(protocol.PackERR(e))
}

// ResetSeq reset sequence to zero.
func (p *Packets) ResetSeq() {
	p.seq = 0
}

// WriteOK writes OK packet to the wire.
func (p *Packets) WriteOK(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	ok := &protocol.OK{
		AffectedRows: affectedRows,
		LastInsertID: lastInsertID,
		StatusFlags:  flags,
		Warnings:     warnings,
	}
	return p.Write(protocol.PackOK(ok))
}

// ReadOK used to read the OK packet.
func (p *Packets) ReadOK() error {
	// EOF packet
	data, err := p.Next()
	if err != nil {
		return err
	}
	switch data[0] {
	case protocol.OK_PACKET:
		return nil
	case protocol.ERR_PACKET:
		return p.ParseERR(data)
	default:
		return sqlerror.NewSQLErrorf(sqlerror.ER_MALFORMED_PACKET, "unexpected.ok.packet[%+v]", data)
	}
}

// ReadEOF used to read the EOF packet.
func (p *Packets) ReadEOF() error {
	// EOF packet
	data, err := p.Next()
	if err != nil {
		return err
	}
	switch data[0] {
	case protocol.EOF_PACKET:
		return nil
	case protocol.ERR_PACKET:
		return p.ParseERR(data)
	default:
		return sqlerror.NewSQLErrorf(sqlerror.ER_MALFORMED_PACKET, "unexpected.eof.packet[%+v]", data)
	}
}

// AppendEOF appends EOF packet to the stream buffer.
func (p *Packets) AppendEOF(flags uint16, warnings uint16) error {
	eof := &protocol.EOF{
		StatusFlags: flags,
		Warnings:    warnings,
	}
	return p.Append(protocol.PackEOF(eof))
}

// AppendOKWithEOFHeader appends OK packet to the stream buffer with EOF header.
func (p *Packets) AppendOKWithEOFHeader(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	ok := &protocol.OK{
		AffectedRows: affectedRows,
		LastInsertID: lastInsertID,
		StatusFlags:  flags,
		Warnings:     warnings,
	}
	buf := common.NewBuffer(64)
	buf.WriteU8(protocol.EOF_PACKET)
	buf.WriteBytes(protocol.PackOK(ok))
	return p.Append(buf.Data())
}

// AppendColumns used to append column to columns.
func (p *Packets) AppendColumns(columns []*protocol.Field) error {
	// column count
	count := len(columns)
	buf := common.NewBuffer(64)
	buf.WriteLenEncode(uint64(count))
	if err := p.Append(buf.Data()); err != nil {
		return err
	}

	// columns info
	for i := 0; i < count; i++ {
		buf := common.NewBuffer(64)
		buf.WriteBytes(PackColumn(columns[i]))
		if err := p.Append(buf.Data()); err != nil {
			return err
		}
	}
	return nil
}

// Flush writes all append-packets to the wire.
func (p *Packets) Flush() error {
	return p.stream.Flush()
}

// PackColumn used to pack the column packet.
func PackColumn(field *protocol.Field) []byte {
	typ, flags := TypeToMySQL(field.Type)
	if field.Flags != 0 {
		flags = int64(field.Flags)
	}

	buf := common.NewBuffer(256)

	// lenenc_str Catalog, always 'def'
	buf.WriteLenEncodeString("def")

	// lenenc_str Schema
	buf.WriteLenEncodeString(field.Database)

	// lenenc_str Table
	buf.WriteLenEncodeString(field.Table)

	// lenenc_str Org_Table
	buf.WriteLenEncodeString(field.OrgTable)

	// lenenc_str Name
	buf.WriteLenEncodeString(field.Name)

	// lenenc_str Org_Name
	buf.WriteLenEncodeString(field.OrgName)

	// lenenc_int length of fixed-length fields [0c]
	buf.WriteLenEncode(uint64(0x0c))

	// 2 character set
	buf.WriteU16(uint16(field.Charset))

	// 4 column length
	buf.WriteU32(field.ColumnLength)

	// 1 type
	buf.WriteU8(byte(typ))

	// 2 flags
	buf.WriteU16(uint16(flags))

	//1 Decimals
	buf.WriteU8(uint8(field.Decimals))

	// 2 filler [00] [00]
	buf.WriteU16(uint16(0))
	return buf.Data()
}

// TypeToMySQL returns the equivalent mysql type and flag for a vitess type.
func TypeToMySQL(typ protocol.Type) (mysqlType, flags int64) {
	val := protocol.TypeToMySQL[typ]
	return val.Typ, val.Flags
}
