package discordgo

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// A scripted server over net.Pipe exercises the real websocket codec without
// ports, credentials, databases, or access to Discord.
type testGateway struct {
	conn    net.Conn
	reader  *bufio.Reader
	packets chan gatewayTestPacket
	done    chan struct{}
}

type gatewayTestPacket struct {
	event *Event
	err   error
}

func (g *testGateway) send(packet string) error {
	header := []byte{0x81}
	if len(packet) < 126 {
		header = append(header, byte(len(packet)))
	} else {
		header = append(header, 126, byte(len(packet)>>8), byte(len(packet)))
	}
	_, err := g.conn.Write(append(header, []byte(packet)...))
	return err
}

func (g *testGateway) readFrame() (*Event, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(g.reader, header); err != nil {
		return nil, err
	}
	if header[0]&15 == websocket.CloseMessage {
		return nil, io.EOF
	}
	length := int(header[1] & 127)
	if length == 126 {
		var n uint16
		if err := binary.Read(g.reader, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		length = int(n)
	} else if length == 127 {
		return nil, errors.New("unexpected large test frame")
	}
	var mask [4]byte
	if header[1]&128 != 0 {
		if _, err := io.ReadFull(g.reader, mask[:]); err != nil {
			return nil, err
		}
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(g.reader, data); err != nil {
		return nil, err
	}
	if header[1]&128 != 0 {
		for i := range data {
			data[i] ^= mask[i%4]
		}
	}
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

func (g *testGateway) hello() error {
	g.packets = make(chan gatewayTestPacket, 32)
	g.done = make(chan struct{})
	go func() {
		defer close(g.done)
		defer close(g.packets)
		for {
			event, err := g.readFrame()
			g.packets <- gatewayTestPacket{event, err}
			if err != nil {
				return
			}
		}
	}()
	return g.send(`{"op":10,"d":{"heartbeat_interval":1000}}`)
}

func (g *testGateway) read() (*Event, error) {
	packet, ok := <-g.packets
	if !ok {
		return nil, io.EOF
	}
	return packet.event, packet.err
}

func (g *testGateway) auth() (int, error) {
	for {
		e, err := g.read()
		if err != nil {
			return 0, err
		}
		if e.Operation == 1 {
			if err := g.send(`{"op":11,"d":null}`); err != nil {
				return 0, err
			}
		} else {
			return e.Operation, nil
		}
	}
}

func (g *testGateway) ready() error {
	return g.send(`{"op":0,"s":1,"t":"READY","d":{"session_id":"synthetic","resume_gateway_url":"ws://resume.invalid/","user":{"id":"1"},"guilds":[]}}`)
}

func (g *testGateway) ackLoop() {
	for {
		e, err := g.read()
		if err != nil {
			return
		}
		if e.Operation == 1 {
			if g.send(`{"op":11,"d":null}`) != nil {
				return
			}
		}
	}
}

type testGatewayTransport struct{}

func (testGatewayTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"url":"ws://fake.invalid/"}`))}, nil
}

func scriptedGateway(t *testing.T, scripts ...func(*testGateway)) (*Session, *int32) {
	t.Helper()
	s, err := New("Bot synthetic-not-a-credential")
	if err != nil {
		t.Fatal(err)
	}
	s.gateway = "ws://fake.invalid/"
	s.Client = &http.Client{Transport: testGatewayTransport{}}
	s.ShouldReconnectVoiceOnSessionError = false
	var count int32
	var mu sync.Mutex
	var pipes []net.Conn
	s.Dialer = &websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		index := int(atomic.AddInt32(&count, 1)) - 1
		if index >= len(scripts) {
			return nil, errors.New("test gateway scripts exhausted")
		}
		client, server := net.Pipe()
		mu.Lock()
		pipes = append(pipes, client, server)
		mu.Unlock()
		go func() {
			defer server.Close()
			reader := bufio.NewReader(server)
			req, err := http.ReadRequest(reader)
			if err != nil {
				return
			}
			hash := sha1.Sum([]byte(req.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			_, err = fmt.Fprintf(server, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(hash[:]))
			if err == nil {
				scripts[index](&testGateway{conn: server, reader: reader})
			}
		}()
		return client, nil
	}}
	t.Cleanup(func() {
		_ = s.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range pipes {
			_ = conn.Close()
		}
	})
	return s, &count
}

func awaitGateway(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("gateway condition was not satisfied")
}

func healthyTestGateway(g *testGateway) {
	if g.hello() != nil {
		return
	}
	if _, err := g.auth(); err != nil {
		return
	}
	if g.ready() == nil {
		g.ackLoop()
	}
}

func TestGatewayReconnectDuringHandshakeReturns(t *testing.T) {
	for _, phase := range []string{"before_hello", "after_hello"} {
		t.Run(phase, func(t *testing.T) {
			s, _ := scriptedGateway(t, func(g *testGateway) {
				if phase == "after_hello" {
					_ = g.hello()
					_, _ = g.auth()
				}
				_ = g.send(`{"op":7,"d":null}`)
			})
			done := make(chan error, 1)
			go func() { done <- s.Open() }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("reconnect request accepted as success")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Open deadlocked on reconnect instruction")
			}
			if s.GatewayHealth().Ready {
				t.Fatal("failed handshake marked ready")
			}
		})
	}
}

func TestGatewayHandshakeACKAndReplay(t *testing.T) {
	var replayed int32
	s, _ := scriptedGateway(t, func(g *testGateway) {
		_ = g.hello()
		_, _ = g.auth()
		_ = g.send(`{"op":1,"d":null}`)
		// Read the requested heartbeat and ACK it before RESUMED.
		for {
			e, err := g.read()
			if err != nil {
				return
			}
			if e.Operation == 1 {
				break
			}
		}
		_ = g.send(`{"op":11,"d":null}`)
		_ = g.send(`{"op":0,"s":2,"t":"MESSAGE_CREATE","d":{"id":"2","channel_id":"3","author":{"id":"4"}}}`)
		_ = g.send(`{"op":0,"s":3,"t":"RESUMED","d":{}}`)
		g.ackLoop()
	})
	s.sessionID = "previous"
	s.AddHandler(func(*Session, *MessageCreate) { atomic.AddInt32(&replayed, 1) })
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	awaitGateway(t, func() bool { return atomic.LoadInt32(&replayed) == 1 && s.GatewayHealth().Online(time.Now()) })
}

func TestGatewayInvalidSessionRetriesFreshIdentify(t *testing.T) {
	for _, resumable := range []bool{false, true} {
		t.Run(fmt.Sprint(resumable), func(t *testing.T) {
			operations := make(chan int, 2)
			s, _ := scriptedGateway(t, func(g *testGateway) {
				_ = g.hello()
				op, _ := g.auth()
				operations <- op
				_ = g.send(fmt.Sprintf(`{"op":9,"d":%t}`, resumable))
			}, func(g *testGateway) {
				_ = g.hello()
				op, _ := g.auth()
				operations <- op
				_ = g.ready()
				g.ackLoop()
			})
			s.sessionID = "old-session"
			var identifies int32
			s.IdentifyGate = func(context.Context) (func(), error) { atomic.AddInt32(&identifies, 1); return func() {}, nil }
			if err := s.Open(); err == nil {
				t.Fatal("invalid session accepted as success")
			}
			if s.GatewayHealth().Ready {
				t.Fatal("invalid session marked ready")
			}
			if err := s.Open(); err != nil {
				t.Fatal(err)
			}
			want := 2
			if resumable {
				want = 6
			}
			if first, second := <-operations, <-operations; first != 6 || second != want {
				t.Fatalf("auth operations %d,%d; want 6,%d", first, second, want)
			}
			wantIdentify := int32(1)
			if resumable {
				wantIdentify = 0
			}
			if atomic.LoadInt32(&identifies) != wantIdentify {
				t.Fatal("fresh Identify pacing hook not respected")
			}
		})
	}
}

func TestGatewayRetrySurvivesImmediateReplacementFailure(t *testing.T) {
	drop := make(chan struct{})
	s, count := scriptedGateway(t, func(g *testGateway) {
		_ = g.hello()
		_, _ = g.auth()
		_ = g.ready()
		<-drop
		_ = g.send(`{"op":7,"d":null}`)
	}, func(g *testGateway) { _ = g.send(`{"op":7,"d":null}`) },
		func(g *testGateway) {
			_ = g.hello()
			_, _ = g.auth()
			_ = g.ready()
			_ = g.send(`{"op":7,"d":null}`)
		}, healthyTestGateway)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	close(drop)
	awaitGateway(t, func() bool { return atomic.LoadInt32(count) == 4 && s.GatewayHealth().Online(time.Now()) })
}

func TestGatewaySilentHandshakeIsBounded(t *testing.T) {
	for _, phase := range []string{"hello", "ready"} {
		t.Run(phase, func(t *testing.T) {
			s, _ := scriptedGateway(t, func(g *testGateway) {
				if phase == "ready" {
					_ = g.hello()
					_, _ = g.auth()
				}
				if g.done != nil {
					<-g.done
				} else {
					_, _ = io.Copy(io.Discard, g.reader)
				}
			})
			s.Dialer.HandshakeTimeout = 80 * time.Millisecond
			start := time.Now()
			if err := s.Open(); err == nil {
				t.Fatal("silent gateway accepted")
			}
			if time.Since(start) > time.Second {
				t.Fatal("handshake not bounded")
			}
			if s.GatewayHealth().Ready {
				t.Fatal("timed-out gateway marked ready")
			}
		})
	}
}

func TestGatewayHealthDoesNotWaitForSessionLock(t *testing.T) {
	s, _ := New("synthetic")
	s.Lock()
	done := make(chan GatewayHealth, 1)
	go func() { done <- s.GatewayHealth() }()
	select {
	case h := <-done:
		if h.Online(time.Now()) {
			t.Fatal("new session considered online")
		}
	case <-time.After(time.Second):
		s.Unlock()
		t.Fatal("health blocked on lifecycle mutex")
	}
	s.Unlock()
}

func TestGatewayHealthNeedsRealACK(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name   string
		health GatewayHealth
		want   bool
	}{
		{"new", GatewayHealth{}, false},
		{"hello_only", GatewayHealth{HeartbeatInterval: time.Second}, false},
		{"ready_without_ack", GatewayHealth{Ready: true, HeartbeatInterval: time.Second}, false},
		{"quiet_healthy", GatewayHealth{true, now.Add(-time.Second), time.Second}, true},
		{"stale", GatewayHealth{true, now.Add(-8 * time.Second), time.Second}, false},
		{"disconnected", GatewayHealth{false, now, time.Second}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.health.Online(now); got != tc.want {
				t.Fatalf("online=%t want %t", got, tc.want)
			}
		})
	}
}

func TestGatewayCloseCancelsIdentifyAndRetry(t *testing.T) {
	t.Run("identify", func(t *testing.T) {
		s, _ := scriptedGateway(t)
		entered := make(chan struct{})
		s.IdentifyGate = func(ctx context.Context) (func(), error) { close(entered); <-ctx.Done(); return nil, ctx.Err() }
		done := make(chan error, 1)
		go func() { done <- s.Open() }()
		<-entered
		_ = s.Close()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("Identify gate not cancelled")
		}
	})
	t.Run("retry", func(t *testing.T) {
		s, count := scriptedGateway(t, func(g *testGateway) { _ = g.send(`{"op":7,"d":null}`) }, func(g *testGateway) { _ = g.send(`{"op":7,"d":null}`) })
		_ = s.Open()
		done := make(chan struct{})
		go func() { s.reconnect(s.gatewayContext); close(done) }()
		awaitGateway(t, func() bool { return atomic.LoadInt32(count) >= 2 })
		_ = s.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("retry backoff not cancelled")
		}
	})
}

func TestGatewayOldConnectionCannotAffectReplacement(t *testing.T) {
	s, _ := scriptedGateway(t, healthyTestGateway)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	awaitGateway(t, func() bool { return s.GatewayHealth().Online(time.Now()) })
	old := &websocket.Conn{}
	s.reconnectGateway(old, errors.New("old reader failed"))
	if _, err := s.onEvent(old, websocket.TextMessage, []byte(`{"op":11,"d":null}`)); err == nil {
		t.Fatal("old connection ACK accepted")
	}
	if !s.GatewayHealth().Online(time.Now()) {
		t.Fatal("old connection damaged replacement")
	}
}

func TestGatewayCloseInterruptsBlockedWriter(t *testing.T) {
	s, _ := scriptedGateway(t, func(g *testGateway) {
		_ = g.send(`{"op":10,"d":{"heartbeat_interval":1000}}`)
		time.Sleep(2 * time.Second)
	})
	done := make(chan error, 1)
	go func() { done <- s.Open() }()
	awaitGateway(t, func() bool { return s.GatewayHealth().HeartbeatInterval > 0 })
	start := time.Now()
	_ = s.Close()
	if time.Since(start) > 1500*time.Millisecond {
		t.Fatal("Close waited for blocked writer")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked handshake succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("Open still blocked after Close")
	}
}

func TestGatewayConcurrentFailuresHaveOneRetryOwner(t *testing.T) {
	s, count := scriptedGateway(t, healthyTestGateway, healthyTestGateway)
	addresses := make(chan string, 4)
	dial := s.Dialer.NetDialContext
	s.Dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		addresses <- address
		return dial(ctx, network, address)
	}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	awaitGateway(t, func() bool { return s.GatewayHealth().Online(time.Now()) })
	s.gatewayMu.RLock()
	old := s.gatewayConn
	s.gatewayMu.RUnlock()
	var failures sync.WaitGroup
	for i := 0; i < 16; i++ {
		failures.Add(1)
		go func() {
			defer failures.Done()
			s.reconnectGateway(old, errors.New("simultaneous read/heartbeat failure"))
		}()
	}
	failures.Wait()
	awaitGateway(t, func() bool { return s.GatewayHealth().Online(time.Now()) })
	if got := atomic.LoadInt32(count); got != 2 {
		t.Fatalf("concurrent failures created %d connections, want 2", got)
	}
	if first, second := <-addresses, <-addresses; first != "fake.invalid:80" || second != "resume.invalid:80" {
		t.Fatalf("resume ignored the READY gateway URL: %s, %s", first, second)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.GatewayHealth().Online(time.Now()) {
		t.Fatal("explicitly closed shard reported online")
	}
}

func TestGatewayStaleHeartbeatReconnects(t *testing.T) {
	stopACK := make(chan struct{})
	s, count := scriptedGateway(t, func(g *testGateway) {
		_ = g.hello()
		_, _ = g.auth()
		_ = g.ready()
		_ = g.send(`{"op":11,"d":null}`)
		<-stopACK
		// Keep accepting frames but stop answering heartbeats.
		for {
			if _, err := g.read(); err != nil {
				return
			}
		}
	}, healthyTestGateway)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	awaitGateway(t, func() bool { return s.GatewayHealth().Online(time.Now()) })
	close(stopACK)
	// Advance the ACK age to avoid spending a real timeout in this test.
	s.gatewayMu.Lock()
	s.gatewayHealth.LastHeartbeatAck = time.Now().Add(-time.Minute)
	s.gatewayMu.Unlock()
	awaitGateway(t, func() bool { return atomic.LoadInt32(count) == 2 && s.GatewayHealth().Online(time.Now()) })
}

func TestGatewayInvalidReadyIsNotAccepted(t *testing.T) {
	s, _ := scriptedGateway(t, func(g *testGateway) {
		_ = g.hello()
		_, _ = g.auth()
		_ = g.send(`{"op":0,"s":1,"t":"READY","d":{"session_id":123}}`)
		g.ackLoop()
	})
	if err := s.Open(); err == nil {
		t.Fatal("malformed READY accepted as a confirmed session")
	}
	if s.GatewayHealth().Ready {
		t.Fatal("malformed READY marked healthy")
	}
}
