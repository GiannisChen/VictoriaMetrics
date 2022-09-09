package vmsqlserver

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/protocol"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/sqlerror"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

// Server is a vmsql server listen on tcp with MySQL protocol.
// ref https://dev.mysql.com/doc/internals/en/client-server-protocol.html
// Server will be a global variant and init only once.
type Server struct {
	version      string
	addr         string
	listener     *netutil.TCPListener
	wg           sync.WaitGroup
	connectionID uint32

	mu sync.RWMutex
	ss map[uint32]*Session
}

// ServerVersion returns the version name.
func (s *Server) ServerVersion() string {
	return s.version
}

func (s *Server) SetServerVersion(v string) {
	if v != "" {
		s.version = v
	}
}

// MustStart init the Server and start the listening loop.
func MustStart(addr, version string) *Server {
	logger.Infof("starting TCP vmsql server at %q", addr)
	lnTCP, err := netutil.NewTCPListener("vmsql", addr)
	if err != nil {
		logger.Fatalf("cannot start TCP vmsql server at %q: %s", addr, err)
	}

	s := &Server{
		addr:     addr,
		version:  version,
		listener: lnTCP,
		ss:       make(map[uint32]*Session),
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.ServeTCP()
		logger.Infof("stopped TCP vmsql server at %q", addr)
	}()
	return s
}

// MustStop stops the Server.
func (s *Server) MustStop() {
	logger.Infof("stopping TCP vmsql server at %q...", s.addr)
	if err := s.listener.Close(); err != nil {
		logger.Errorf("cannot close TCP vmsql server: %s", err)
	}
	s.wg.Wait()
	logger.Infof("vmsql servers at %q have been stopped", s.addr)
}

func (s *Server) ServeTCP() {
	var wg sync.WaitGroup
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) {
				if ne.Temporary() {
					logger.Errorf("vmsql: temporary error when listening for TCP addr %q: %s", s.listener.Addr(), err)
					time.Sleep(time.Second)
					continue
				}
				if strings.Contains(err.Error(), "use of closed network connection") {
					break
				}
				logger.Fatalf("unrecoverable error when accepting TCP vmsql connections: %s", err)
			}
			logger.Fatalf("unexpected error when accepting TCP vmsql connections: %s", err)
		}
		id := s.connectionID
		s.connectionID++
		s.wg.Add(1)
		// s.wg.Done() in handle routine.
		go s.handle(conn, id)
	}
	wg.Wait()
}

func (s *Server) handle(conn net.Conn, id uint32) {
	// Catch panics, and close the connection in any case.
	defer func() {
		conn.Close()
		if x := recover(); x != nil {
			logger.Errorf("server.handle.panic:\n%v\n%s", x, debug.Stack())
		}
		s.wg.Done()
	}()
	var err error
	var data []byte
	var authPkt []byte
	var greetingPkt []byte

	// Skip session check.
	// Session register.
	session := newSession(id, s.ServerVersion(), conn)
	s.RegisterSession(session)
	defer s.SessionClosed(session)

	// Greeting packet.
	greetingPkt = session.greeting.Pack()
	if err = session.packets.Write(greetingPkt); err != nil {
		logger.Errorf("server.write.greeting.packet.error: %v", err)
		return
	}

	// Auth packet.
	if authPkt, err = session.packets.Next(); err != nil {
		logger.Errorf("server.read.auth.packet.error: %v", err)
		return
	}
	if err = session.auth.UnPack(authPkt); err != nil {
		logger.Errorf("server.unpack.auth.error: %v", err)
		return
	}

	//  Auth check.
	if err = s.AuthCheck(session); err != nil {
		logger.Warnf("server.user[%+v].auth.check.failed", session.User())
		session.writeErrFromError(err)
		return
	}

	// Check the database.
	db := session.auth.Database()
	if db != "timeseries" {
		logger.Errorf("server.cominit.db.error: unknown database [%s]", db)
		session.writeErrFromError(fmt.Errorf("mock.cominit.db.error: unknown database[%s]", db))
		return
	}

	if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
		return
	}

	// Reset packet sequence ID.
	session.packets.ResetSeq()
	for {
		if data, err = session.packets.Next(); err != nil {
			return
		}

		// Update the session last query time for session idle.
		session.updateLastQueryTime(time.Now())
		switch data[0] {
		// COM_QUIT
		case protocol.COM_QUIT:
			return
			// COM_INIT_DB
		case protocol.COM_INIT_DB:
			db := data[1:]
			if string(db) != "timeseries" {
				logger.Errorf("server.cominit.db.error: unknown database [%s]", db)
				session.writeErrFromError(fmt.Errorf("mock.cominit.db.error: unknown database[%s]", db))
				return
			} else {
				if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
					return
				}
			}
			// COM_PING
		case protocol.COM_PING:
			if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
				return
			}
			// COM_QUERY
		case protocol.COM_QUERY:
			query := parserComQuery(data)
			if err = s.ComQuery(session, query, func(qr protocol.Result) error {
				return session.WriteTextRows(qr)
			}); err != nil {
				logger.Errorf("server handle query from session[%v] error:%+v query[%s]", id, err, query)
				if werr := session.writeErrFromError(err); werr != nil {
					return
				}
			}
		default:
			cmd := protocol.CommandString(data[0])
			logger.Errorf("session.command:%s.not.implemented", cmd)
			sqlErr := sqlerror.NewSQLErrorf(sqlerror.ER_UNKNOWN_ERROR, "command handling not implemented yet: %s", cmd)
			if err := session.writeErrFromError(sqlErr); err != nil {
				return
			}
		}
		// Reset packet sequence ID.
		session.packets.ResetSeq()
	}
}

func (s *Server) RegisterSession(session *Session) {
	session.killed = make(chan bool, 2)
	s.mu.Lock()
	s.ss[session.id] = session
	s.mu.Unlock()
}

func (s *Server) SessionClosed(session *Session) {
	s.mu.Lock()
	if _, ok := s.ss[session.id]; !ok {
		s.ss[session.id].Close()
	}
	delete(s.ss, session.id)
	s.mu.Unlock()
}
