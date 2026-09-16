package discordgo

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const gatewayWriteTimeout = 10 * time.Second

// GatewayHealth is a point-in-time snapshot of the current gateway connection.
// A socket or successful heartbeat write alone does not establish readiness.
type GatewayHealth struct {
	Ready             bool
	LastHeartbeatAck  time.Time
	HeartbeatInterval time.Duration
}

// Online reports whether Discord confirmed this session and is still answering
// heartbeats. It does not depend on guild activity or application handlers.
func (h GatewayHealth) Online(now time.Time) bool {
	return h.Ready && h.HeartbeatInterval > 0 && !h.LastHeartbeatAck.IsZero() &&
		now.Sub(h.LastHeartbeatAck) <= 2*h.HeartbeatInterval+5*time.Second
}

// GatewayHealth returns promptly even while Open is waiting for the network.
func (s *Session) GatewayHealth() GatewayHealth {
	s.gatewayMu.RLock()
	defer s.gatewayMu.RUnlock()
	return s.gatewayHealth
}

type gatewayReconnectError struct{ resume bool }

func (e *gatewayReconnectError) Error() string {
	if e.resume {
		return "gateway requested reconnect/resume"
	}
	return "gateway session invalidated; fresh identify required"
}

// Reset session metadata only at the connection owner's failure boundary.
func (s *Session) invalidateGatewaySession(err error) {
	var reconnect *gatewayReconnectError
	var closed *websocket.CloseError
	if (errors.As(err, &reconnect) && !reconnect.resume) ||
		(errors.As(err, &closed) && (closed.Code == 4007 || closed.Code == 4009)) {
		s.gatewayMu.Lock()
		s.sessionID, s.resumeGateway = "", ""
		atomic.StoreInt64(s.sequence, 0)
		s.gatewayMu.Unlock()
	}
}

func (s *Session) gatewayHandshakeTimeout() time.Duration {
	if s.Dialer != nil && s.Dialer.HandshakeTimeout > 0 {
		return s.Dialer.HandshakeTimeout
	}
	return 30 * time.Second
}

// Every gateway data write uses the same mutex and a fresh finite deadline.
// Close uses WriteControl, which can interrupt a blocked data writer safely.
func (s *Session) gatewayWrite(conn *websocket.Conn, payload interface{}) error {
	s.wsMutex.Lock()
	defer s.wsMutex.Unlock()
	if err := conn.SetWriteDeadline(time.Now().Add(gatewayWriteTimeout)); err != nil {
		return err
	}
	return conn.WriteJSON(payload)
}

func (s *Session) gatewayCurrent(conn *websocket.Conn) bool {
	s.gatewayMu.RLock()
	defer s.gatewayMu.RUnlock()
	return s.gatewayConn == conn
}

// Caller holds Session.Lock. Closing this exact socket prevents old readers or
// heartbeats from tearing down its replacement.
func (s *Session) closeGatewayLocked(conn *websocket.Conn, code int) error {
	if conn == nil || s.wsConn != conn {
		return nil
	}
	s.gatewayMu.Lock()
	s.gatewayHealth.Ready = false
	s.gatewayConn = nil
	s.gatewayMu.Unlock()
	s.DataReady = false
	if s.listening != nil {
		close(s.listening)
		s.listening = nil
	}
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, ""), time.Now().Add(time.Second))
	err := conn.Close()
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
		err = nil
	}
	s.wsConn = nil
	return err
}

// The first failure owns teardown and retries. Closing the socket before taking
// Session.Lock also unblocks a handshake read that owns that lock.
func (s *Session) reconnectGateway(conn *websocket.Conn, cause error) {
	s.gatewayMu.Lock()
	if s.gatewayConn != conn {
		s.gatewayMu.Unlock()
		return
	}
	s.gatewayHealth.Ready = false
	s.gatewayMu.Unlock()
	_ = conn.Close()
	s.Lock()
	if s.wsConn != conn {
		s.Unlock()
		return
	}
	s.gatewayMu.RLock()
	ctx := s.gatewayContext
	s.gatewayMu.RUnlock()
	s.invalidateGatewaySession(cause)
	_ = s.closeGatewayLocked(conn, websocket.CloseServiceRestart)
	s.Unlock()
	s.handleEvent(disconnectEventType, &Disconnect{})
	if ctx != nil {
		s.reconnect(ctx)
	}
}

func waitGatewayRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
