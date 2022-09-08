package vmsqlserver

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/packet"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/protocol"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/sqlerror"
	"net"
	"strconv"
	"sync"
	"time"
)

type Session struct {
	id            uint32
	mu            sync.RWMutex
	conn          net.Conn
	lastQueryTime time.Time
	auth          *protocol.Auth
	packets       *packet.Packets
	greeting      *protocol.Greeting

	closed bool
	killed chan bool
}

func newSession(id uint32, version string, conn net.Conn) *Session {
	return &Session{
		id:            id,
		conn:          conn,
		lastQueryTime: time.Now(),

		auth:     protocol.NewAuth(),
		greeting: protocol.NewGreeting(id, version),
		packets:  packet.NewPackets(conn),
	}
}

func (s *Session) User() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.auth.User()
}

// updateLastQueryTime update the lastQueryTime.
func (s *Session) updateLastQueryTime(time time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastQueryTime = time
}

func (s *Session) writeErrFromError(err error) error {
	if se, ok := err.(*sqlerror.SQLError); ok {
		return s.packets.WriteERR(se.Num, se.State, "%v", se.Message)
	}
	unknow := sqlerror.NewSQLErrorf(sqlerror.ER_UNKNOWN_ERROR, "%v", err)
	return s.packets.WriteERR(unknow.Num, unknow.State, unknow.Message)
}

// Close used to close the connection.
func (s *Session) Close() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

func (s *Session) WriteTextRows(result protocol.Result) error {
	if result == nil {
		result = &protocol.MySQLResult{}
	}
	if result.ColumnSize() == 0 {
		if result.GetState() == protocol.RStateNone {
			// This is just an INSERT result, send an OK packet.
			return s.packets.WriteOK(result.GetRowsAffected(), result.GetInsertID(), s.greeting.Status(), result.GetWarnings())
		}
		return fmt.Errorf("unexpected: result.without.no.fields.but.has.rows.result:%+v", result)
	}
	switch result.GetState() {
	case protocol.RStateNone:
		if err := s.writeFields(result); err != nil {
			return err
		}
		if err := s.AppendTextRows(result); err != nil {
			return err
		}
		if err := s.writeFinish(result); err != nil {
			return err
		}
	case protocol.RStateFields:
		if err := s.writeFields(result); err != nil {
			return err
		}
	case protocol.RStateRows:
		if err := s.AppendTextRows(result); err != nil {
			return err
		}
	case protocol.RStateFinished:
		if err := s.writeFinish(result); err != nil {
			return err
		}
	}
	return s.flush()
}

func (s *Session) AppendTextRows(result protocol.Result) error {
	switch result.(type) {
	case *protocol.MySQLResult:
		return s.appendTextRowsFromMySQLResult(result.(*protocol.MySQLResult))
	case *protocol.TimeSeriesResult:
		return s.appendTextRowsFromTSResult(result.(*protocol.TimeSeriesResult))
	default:
		return fmt.Errorf("unexcepted inside error")
	}
}

func (s *Session) appendTextRowsFromMySQLResult(result *protocol.MySQLResult) error {
	for _, row := range result.Rows {
		rowBuf := common.NewBuffer(16)
		for _, val := range row {
			rowBuf.WriteLenEncodeBytes(val)
		}
		if err := s.packets.Append(rowBuf.Data()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) appendTextRowsFromTSResult(result *protocol.TimeSeriesResult) error {
	for _, block := range result.Blocks {
		timeList := make(map[int64]int, 0)
		idx := make([]int, len(block.Data))
		for _, datum := range block.Data {
			for _, timestamp := range datum.Timestamps {
				timeList[timestamp]++
			}
		}
		width := len(block.Data) - 1
		var ts int64
		// Block.Data[0] represents timestamp
		for ; idx[1] < len(block.Data[1].Timestamps); idx[1]++ {
			if c, _ := timeList[block.Data[1].Timestamps[idx[1]]]; c == width {
				rowBuf := common.NewBuffer(16)
				ts = block.Data[1].Timestamps[idx[1]]
				for _, tag := range block.Tags {
					rowBuf.WriteLenEncodeBytes(tag)
				}
				//rowBuf.WriteLenEncodeString(strconv.FormatInt(ts, 10))
				rowBuf.WriteLenEncodeString(time.UnixMilli(ts).Format("2006-01-02 15:04:05.999"))
				for i := 1; i < len(idx); i++ {
					for block.Data[i].Timestamps[idx[i]] < ts {
						idx[i]++
					}
					rowBuf.WriteLenEncodeString(strconv.FormatFloat(block.Data[i].Values[idx[i]], 'f', 100, 64))
				}
				if err := s.packets.Append(rowBuf.Data()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Session) writeFields(result protocol.Result) error {
	// 1. Write columns.
	if err := s.packets.AppendColumns(result.GetFields()); err != nil {
		return err
	}

	if (s.auth.ClientFlags() & protocol.CLIENT_DEPRECATE_EOF) == 0 {
		if err := s.packets.AppendEOF(s.greeting.Status(), result.GetWarnings()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) writeFinish(result protocol.Result) error {
	// 3. Write EOF.
	if (s.auth.ClientFlags() & protocol.CLIENT_DEPRECATE_EOF) == 0 {
		if err := s.packets.AppendEOF(s.greeting.Status(), result.GetWarnings()); err != nil {
			return err
		}
	} else {
		if err := s.packets.AppendOKWithEOFHeader(
			result.GetRowsAffected(), result.GetInsertID(),
			s.greeting.Status(), result.GetWarnings()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) flush() error {
	// 4. Write to stream.
	return s.packets.Flush()
}
