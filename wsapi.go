// Discordgo - Discord bindings for Go
// Available at https://github.com/bwmarrin/discordgo

// Copyright 2015-2016 Bruce Marriner <bruce@sqls.net>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file contains low level functions for interacting with the Discord
// data websocket interface.

package discordgo

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ErrWSAlreadyOpen is thrown when you attempt to open
// a websocket that already is open.
var ErrWSAlreadyOpen = errors.New("web socket already opened")

// ErrWSNotFound is thrown when you attempt to use a websocket
// that doesn't exist
var ErrWSNotFound = errors.New("no websocket connection exists")

// ErrWSShardBounds is thrown when you try to use a shard ID that is
// more than the total shard count
var ErrWSShardBounds = errors.New("ShardID must be less than ShardCount")

type resumePacket struct {
	Op   int `json:"op"`
	Data struct {
		Token     string `json:"token"`
		SessionID string `json:"session_id"`
		Sequence  int64  `json:"seq"`
	} `json:"d"`
}

// Open creates a websocket connection to Discord.
// See: https://discord.com/developers/docs/topics/gateway#connecting
func (s *Session) Open() error {
	return s.openGateway(nil)
}

// A retry supplies its original context so Close cannot accidentally resurrect
// the session. Only an explicit Open starts a new lifetime after Close.
func (s *Session) openGateway(retryContext context.Context) (err error) {
	s.Lock()
	defer s.Unlock()
	if s.wsConn != nil {
		return ErrWSAlreadyOpen
	}

	s.gatewayMu.Lock()
	if retryContext != nil && (retryContext != s.gatewayContext || retryContext.Err() != nil) {
		s.gatewayMu.Unlock()
		return context.Canceled
	}
	if s.gatewayContext == nil || s.gatewayContext.Err() != nil {
		s.gatewayContext, s.gatewayCancel = context.WithCancel(context.Background())
	}
	ctx := s.gatewayContext
	s.gatewayHealth = GatewayHealth{}
	sessionID, resumeURL := s.sessionID, s.resumeGateway
	s.gatewayMu.Unlock()

	resuming := sessionID != ""
	var releaseIdentify func()
	if !resuming && s.IdentifyGate != nil {
		if releaseIdentify, err = s.IdentifyGate(ctx); err != nil {
			return err
		}
		defer func() {
			if releaseIdentify != nil {
				releaseIdentify()
			}
		}()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if s.gateway == "" {
		s.gateway, err = s.Gateway()
		if err != nil {
			return err
		}
		s.gateway += "?v=" + APIVersion + "&encoding=json"
	}
	gateway := s.gateway
	if resuming && resumeURL != "" {
		gateway = resumeURL + "?v=" + APIVersion + "&encoding=json"
	}
	s.log(LogInformational, "connecting to gateway %s (resume=%t)", gateway, resuming)
	header := http.Header{}
	header.Add("accept-encoding", "zlib")
	conn, _, err := s.Dialer.DialContext(ctx, gateway, header)
	if err != nil {
		s.gateway = ""
		return err
	}
	s.wsConn = conn
	s.gatewayMu.Lock()
	s.gatewayConn = conn
	s.gatewayMu.Unlock()
	conn.SetCloseHandler(func(int, string) error { return nil })
	defer func() {
		if err != nil {
			s.invalidateGatewaySession(err)
			_ = s.closeGatewayLocked(conn, websocket.CloseServiceRestart)
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = conn.SetReadDeadline(time.Now().Add(s.gatewayHandshakeTimeout())); err != nil {
		return err
	}
	mt, message, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	// Do not dispatch a control packet while Open owns the session lock.
	e, err := decodeGatewayEvent(mt, message)
	if err != nil {
		return err
	}
	if e.Operation != 10 {
		if e.Operation == 9 {
			var resumable bool
			if err = json.Unmarshal(e.RawData, &resumable); err != nil {
				return err
			}
			return &gatewayReconnectError{resume: resumable}
		}
		return fmt.Errorf("expecting Hello, got gateway opcode %d", e.Operation)
	}
	var hello helloOp
	if err = json.Unmarshal(e.RawData, &hello); err != nil {
		return err
	}
	if hello.HeartbeatInterval <= 0 {
		return errors.New("invalid gateway heartbeat interval")
	}
	interval := hello.HeartbeatInterval * time.Millisecond
	s.gatewayMu.Lock()
	s.gatewayHealth.HeartbeatInterval = interval
	s.LastHeartbeatAck = time.Time{}
	s.LastHeartbeatSent = time.Time{}
	s.gatewayMu.Unlock()
	s.listening = make(chan interface{})
	go s.heartbeat(conn, s.listening, hello.HeartbeatInterval)

	if resuming {
		var packet resumePacket
		packet.Op = 6
		packet.Data.Token, packet.Data.SessionID = s.Token, sessionID
		packet.Data.Sequence = atomic.LoadInt64(s.sequence)
		err = s.gatewayWrite(conn, packet)
	} else {
		err = s.identify()
	}
	if releaseIdentify != nil {
		releaseIdentify()
		releaseIdentify = nil
	}
	if err != nil {
		return err
	}
	if s.State == nil {
		s.State = NewState()
		s.State.TrackChannels = false
		s.State.TrackEmojis = false
		s.State.TrackMembers = false
		s.State.TrackRoles = false
		s.State.TrackVoice = false
	}
	// The absolute deadline bounds even peers sending ACKs without completing
	// authentication. Replay dispatches are delivered before RESUMED as usual.
	if err = conn.SetReadDeadline(time.Now().Add(s.gatewayHandshakeTimeout())); err != nil {
		return err
	}
	for {
		mt, message, err = conn.ReadMessage()
		if err != nil {
			return err
		}
		e, err = s.onEvent(conn, mt, message)
		if err != nil {
			return err
		}
		if e.Operation == 0 && (e.Type == "READY" || e.Type == "RESUMED") {
			break
		}
	}
	if err = conn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	s.DataReady = true
	if s.VoiceConnections == nil {
		s.VoiceConnections = make(map[string]*VoiceConnection)
	}
	s.log(LogInformational, "gateway session confirmed (%s)", e.Type)
	s.handleEvent(connectEventType, &Connect{})
	go s.listen(conn, s.listening)
	return nil
}

// listen polls the websocket connection for events, it will stop when the
// listening channel is closed, or an error occurs.
func (s *Session) listen(conn *websocket.Conn, listening <-chan interface{}) {
	for {
		mt, message, err := conn.ReadMessage()
		if err == nil {
			select {
			case <-listening:
				return
			default:
			}
			_, err = s.onEvent(conn, mt, message)
		}
		if err != nil {
			if s.gatewayCurrent(conn) {
				s.log(LogWarning, "gateway read/control failure: %s", err)
				s.reconnectGateway(conn, err)
			}
			return
		}
	}
}

type heartbeatOp struct {
	Op   int   `json:"op"`
	Data int64 `json:"d"`
}

type helloOp struct {
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
}

// FailedHeartbeatAcks is the Number of heartbeat intervals to wait until forcing a connection restart.
const FailedHeartbeatAcks time.Duration = 5 * time.Millisecond

// HeartbeatLatency returns the latency between heartbeat acknowledgement and heartbeat send.
func (s *Session) HeartbeatLatency() time.Duration {
	s.gatewayMu.RLock()
	defer s.gatewayMu.RUnlock()
	return s.LastHeartbeatAck.Sub(s.LastHeartbeatSent)
}

func (s *Session) heartbeat(conn *websocket.Conn, listening <-chan interface{}, heartbeatIntervalMsec time.Duration) {
	interval := heartbeatIntervalMsec * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	started := time.Now()
	for {
		select {
		case <-listening:
			return
		default:
		}
		if !s.gatewayCurrent(conn) {
			return
		}
		health := s.GatewayHealth()
		last := health.LastHeartbeatAck
		if last.IsZero() {
			last = started
		}
		if time.Since(last) > 2*interval+5*time.Second {
			s.log(LogWarning, "gateway heartbeat ACK stale; reconnecting")
			s.reconnectGateway(conn, errors.New("gateway heartbeat ACK timeout"))
			return
		}
		if err := s.sendHeartbeat(conn); err != nil {
			s.log(LogWarning, "gateway heartbeat write failed: %s", err)
			s.reconnectGateway(conn, err)
			return
		}
		select {
		case <-ticker.C:
		case <-listening:
			return
		}
	}
}

func (s *Session) sendHeartbeat(conn *websocket.Conn) error {
	s.gatewayMu.Lock()
	if s.gatewayConn != conn {
		s.gatewayMu.Unlock()
		return ErrWSNotFound
	}
	s.LastHeartbeatSent = time.Now().UTC()
	s.gatewayMu.Unlock()
	return s.gatewayWrite(conn, heartbeatOp{1, atomic.LoadInt64(s.sequence)})
}

// UpdateStatusData is provided to UpdateStatusComplex()
type UpdateStatusData struct {
	IdleSince  *int        `json:"since"`
	Activities []*Activity `json:"activities"`
	AFK        bool        `json:"afk"`
	Status     string      `json:"status"`
}

type updateStatusOp struct {
	Op   int              `json:"op"`
	Data UpdateStatusData `json:"d"`
}

func newUpdateStatusData(idle int, activityType ActivityType, name, url string) *UpdateStatusData {
	usd := &UpdateStatusData{
		Status: "online",
	}

	if idle > 0 {
		usd.IdleSince = &idle
	}

	if name != "" {
		usd.Activities = []*Activity{{
			Name: name,
			Type: activityType,
			URL:  url,
		}}
	}

	return usd
}

// UpdateGameStatus is used to update the user's status.
// If idle>0 then set status to idle.
// If name!="" then set game.
// if otherwise, set status to active, and no activity.
func (s *Session) UpdateGameStatus(idle int, name string) (err error) {
	return s.UpdateStatusComplex(*newUpdateStatusData(idle, ActivityTypeGame, name, ""))
}

// UpdateWatchStatus is used to update the user's watch status.
// If idle>0 then set status to idle.
// If name!="" then set movie/stream.
// if otherwise, set status to active, and no activity.
func (s *Session) UpdateWatchStatus(idle int, name string) (err error) {
	return s.UpdateStatusComplex(*newUpdateStatusData(idle, ActivityTypeWatching, name, ""))
}

// UpdateStreamingStatus is used to update the user's streaming status.
// If idle>0 then set status to idle.
// If name!="" then set game.
// If name!="" and url!="" then set the status type to streaming with the URL set.
// if otherwise, set status to active, and no game.
func (s *Session) UpdateStreamingStatus(idle int, name string, url string) (err error) {
	gameType := ActivityTypeGame
	if url != "" {
		gameType = ActivityTypeStreaming
	}
	return s.UpdateStatusComplex(*newUpdateStatusData(idle, gameType, name, url))
}

// UpdateListeningStatus is used to set the user to "Listening to..."
// If name!="" then set to what user is listening to
// Else, set user to active and no activity.
func (s *Session) UpdateListeningStatus(name string) (err error) {
	return s.UpdateStatusComplex(*newUpdateStatusData(0, ActivityTypeListening, name, ""))
}

// UpdateCustomStatus is used to update the user's custom status.
// If state!="" then set the custom status.
// Else, set user to active and remove the custom status.
func (s *Session) UpdateCustomStatus(state string) (err error) {
	data := UpdateStatusData{
		Status: "online",
	}

	if state != "" {
		// Discord requires a non-empty activity name, therefore we provide "Custom Status" as a placeholder.
		data.Activities = []*Activity{{
			Name:  "Custom Status",
			Type:  ActivityTypeCustom,
			State: state,
		}}
	}

	return s.UpdateStatusComplex(data)
}

// UpdateStatusComplex allows for sending the raw status update data untouched by discordgo.
func (s *Session) UpdateStatusComplex(usd UpdateStatusData) (err error) {
	// The comment does say "untouched by discordgo", but we might need to lie a bit here.
	// The Discord documentation lists `activities` as being nullable, but in practice this
	// doesn't seem to be the case. I had filed an issue about this at
	// https://github.com/discord/discord-api-docs/issues/2559, but as of writing this
	// haven't had any movement on it, so at this point I'm assuming this is an error,
	// and am fixing this bug accordingly. Because sending `null` for `activities` instantly
	// disconnects us, I think that disallowing it from being sent in `UpdateStatusComplex`
	// isn't that big of an issue.
	if usd.Activities == nil {
		usd.Activities = make([]*Activity, 0)
	}

	s.RLock()
	defer s.RUnlock()
	if s.wsConn == nil {
		return ErrWSNotFound
	}

	err = s.gatewayWrite(s.wsConn, updateStatusOp{3, usd})

	return
}

type requestGuildMembersData struct {
	// TODO: Deprecated. Use string instead of []string
	GuildIDs  []string  `json:"guild_id"`
	Query     *string   `json:"query,omitempty"`
	UserIDs   *[]string `json:"user_ids,omitempty"`
	Limit     int       `json:"limit"`
	Nonce     string    `json:"nonce,omitempty"`
	Presences bool      `json:"presences"`
}

type requestGuildMembersOp struct {
	Op   int                     `json:"op"`
	Data requestGuildMembersData `json:"d"`
}

// RequestGuildMembers requests guild members from the gateway
// The gateway responds with GuildMembersChunk events
// guildID   : Single Guild ID to request members of
// query     : String that username starts with, leave empty to return all members
// limit     : Max number of items to return, or 0 to request all members matched
// nonce     : Nonce to identify the Guild Members Chunk response
// presences : Whether to request presences of guild members
func (s *Session) RequestGuildMembers(guildID, query string, limit int, nonce string, presences bool) error {
	return s.RequestGuildMembersBatch([]string{guildID}, query, limit, nonce, presences)
}

// RequestGuildMembersList requests guild members from the gateway
// The gateway responds with GuildMembersChunk events
// guildID   : Single Guild ID to request members of
// userIDs   : IDs of users to fetch
// limit     : Max number of items to return, or 0 to request all members matched
// nonce     : Nonce to identify the Guild Members Chunk response
// presences : Whether to request presences of guild members
func (s *Session) RequestGuildMembersList(guildID string, userIDs []string, limit int, nonce string, presences bool) error {
	return s.RequestGuildMembersBatchList([]string{guildID}, userIDs, limit, nonce, presences)
}

// RequestGuildMembersBatch requests guild members from the gateway
// The gateway responds with GuildMembersChunk events
// guildID   : Slice of guild IDs to request members of
// query     : String that username starts with, leave empty to return all members
// limit     : Max number of items to return, or 0 to request all members matched
// nonce     : Nonce to identify the Guild Members Chunk response
// presences : Whether to request presences of guild members
//
// NOTE: this function is deprecated, please use RequestGuildMembers instead
func (s *Session) RequestGuildMembersBatch(guildIDs []string, query string, limit int, nonce string, presences bool) (err error) {
	data := requestGuildMembersData{
		GuildIDs:  guildIDs,
		Query:     &query,
		Limit:     limit,
		Nonce:     nonce,
		Presences: presences,
	}
	err = s.requestGuildMembers(data)
	return
}

// RequestGuildMembersBatchList requests guild members from the gateway
// The gateway responds with GuildMembersChunk events
// guildID   : Slice of guild IDs to request members of
// userIDs   : IDs of users to fetch
// limit     : Max number of items to return, or 0 to request all members matched
// nonce     : Nonce to identify the Guild Members Chunk response
// presences : Whether to request presences of guild members
//
// NOTE: this function is deprecated, please use RequestGuildMembersList instead
func (s *Session) RequestGuildMembersBatchList(guildIDs []string, userIDs []string, limit int, nonce string, presences bool) (err error) {
	data := requestGuildMembersData{
		GuildIDs:  guildIDs,
		UserIDs:   &userIDs,
		Limit:     limit,
		Nonce:     nonce,
		Presences: presences,
	}
	err = s.requestGuildMembers(data)
	return
}

func (s *Session) requestGuildMembers(data requestGuildMembersData) (err error) {
	s.log(LogDebug, "called")

	s.RLock()
	defer s.RUnlock()
	if s.wsConn == nil {
		return ErrWSNotFound
	}

	err = s.gatewayWrite(s.wsConn, requestGuildMembersOp{8, data})

	return
}

// Decode without dispatching control packets into lifecycle methods. In
// particular, Open must be able to reject a non-Hello packet without reentry.
func decodeGatewayEvent(messageType int, message []byte) (*Event, error) {
	var reader io.Reader = bytes.NewReader(message)
	if messageType == websocket.BinaryMessage {
		z, err := zlib.NewReader(reader)
		if err != nil {
			return nil, err
		}
		defer z.Close()
		reader = z
	}
	var event *Event
	if err := json.NewDecoder(reader).Decode(&event); err != nil {
		return nil, err
	}
	if event == nil {
		return nil, errors.New("null gateway event")
	}
	return event, nil
}

func (s *Session) onEvent(conn *websocket.Conn, messageType int, message []byte) (*Event, error) {
	e, err := decodeGatewayEvent(messageType, message)
	if err != nil {
		return nil, err
	}
	if !s.gatewayCurrent(conn) {
		return e, ErrWSNotFound
	}
	s.log(LogDebug, "Op: %d, Seq: %d, Type: %s, Data: %s\n\n", e.Operation, e.Sequence, e.Type, string(e.RawData))

	// Return control to Open/listen; closing or recursively reopening here
	// deadlocks when Open owns Session.Lock during the handshake.
	switch e.Operation {
	case 1:
		return e, s.sendHeartbeat(conn)
	case 7:
		return e, &gatewayReconnectError{resume: true}
	case 9:
		var resumable bool
		if err := json.Unmarshal(e.RawData, &resumable); err != nil {
			return e, err
		}
		return e, &gatewayReconnectError{resume: resumable}
	case 10:
		return e, nil
	case 11:
		s.gatewayMu.Lock()
		if s.gatewayConn == conn {
			s.LastHeartbeatAck = time.Now().UTC()
			s.gatewayHealth.LastHeartbeatAck = s.LastHeartbeatAck
		}
		s.gatewayMu.Unlock()
		return e, nil
	}

	// Do not try to Dispatch a non-Dispatch Message
	if e.Operation != 0 {
		// But we probably should be doing something with them.
		// TEMP
		s.log(LogWarning, "unknown Op: %d, Seq: %d, Type: %s, Data: %s, message: %s", e.Operation, e.Sequence, e.Type, string(e.RawData), string(message))
		return e, nil
	}

	// Keep an in-flight event from an old reader out of the replacement's resume
	// metadata, even if teardown happened after the initial identity check.
	s.gatewayMu.Lock()
	if s.gatewayConn != conn {
		s.gatewayMu.Unlock()
		return e, ErrWSNotFound
	}
	atomic.StoreInt64(s.sequence, e.Sequence)
	s.gatewayMu.Unlock()

	// Map event to registered event handlers and pass it along to any registered handlers.
	if eh, ok := registeredInterfaceProviders[e.Type]; ok {
		e.Struct = eh.New()

		// Attempt to unmarshal our event.
		if err = json.Unmarshal(e.RawData, e.Struct); err != nil {
			s.log(LogError, "error unmarshalling %s event, %s", e.Type, err)
			if e.Type == "READY" || e.Type == "RESUMED" {
				return e, err
			}
		}

		if err == nil && (e.Type == "READY" || e.Type == "RESUMED") {
			s.gatewayMu.Lock()
			if s.gatewayConn == conn {
				s.gatewayHealth.Ready = true
				if ready, ok := e.Struct.(*Ready); ok {
					s.sessionID, s.resumeGateway = ready.SessionID, ready.ResumeGatewayURL
				}
			}
			s.gatewayMu.Unlock()
		}

		// Send event to any registered event handlers for it's type.
		// Because the above doesn't cancel this, in case of an error
		// the struct could be partially populated or at default values.
		// However, most errors are due to a single field and I feel
		// it's better to pass along what we received than nothing at all.
		// TODO: Think about that decision :)
		// Either way, READY events must fire, even with errors.
		s.handleEvent(e.Type, e.Struct)
	} else {
		s.log(LogWarning, "unknown event: Op: %d, Seq: %d, Type: %s, Data: %s", e.Operation, e.Sequence, e.Type, string(e.RawData))
	}

	// For legacy reasons, we send the raw event also, this could be useful for handling unknown events
	// TK: this is useful. we need to keep this for a number of things, including datadog and sequence tracking
	s.handleEvent(eventEventType, e)

	return e, nil
}

// ------------------------------------------------------------------------------------------------
// Code related to voice connections that initiate over the data websocket
// ------------------------------------------------------------------------------------------------

type voiceChannelJoinData struct {
	GuildID   *string `json:"guild_id"`
	ChannelID *string `json:"channel_id"`
	SelfMute  bool    `json:"self_mute"`
	SelfDeaf  bool    `json:"self_deaf"`
}

type voiceChannelJoinOp struct {
	Op   int                  `json:"op"`
	Data voiceChannelJoinData `json:"d"`
}

// ChannelVoiceJoin joins the session user to a voice channel.
//
//	gID     : Guild ID of the channel to join.
//	cID     : Channel ID of the channel to join.
//	mute    : If true, you will be set to muted upon joining.
//	deaf    : If true, you will be set to deafened upon joining.
func (s *Session) ChannelVoiceJoin(gID, cID string, mute, deaf bool) (voice *VoiceConnection, err error) {

	s.log(LogDebug, "called")

	s.RLock()
	voice, _ = s.VoiceConnections[gID]
	s.RUnlock()

	if voice == nil {
		voice = &VoiceConnection{}
		s.Lock()
		s.VoiceConnections[gID] = voice
		s.Unlock()
	}

	voice.Lock()
	voice.GuildID = gID
	voice.ChannelID = cID
	voice.deaf = deaf
	voice.mute = mute
	voice.session = s
	voice.Unlock()

	err = s.ChannelVoiceJoinManual(gID, cID, mute, deaf)
	if err != nil {
		return
	}

	// doesn't exactly work perfect yet.. TODO
	err = voice.waitUntilConnected()
	if err != nil {
		s.log(LogWarning, "error waiting for voice to connect, %s", err)
		voice.Close()
		return
	}

	return
}

// ChannelVoiceJoinManual initiates a voice session to a voice channel, but does not complete it.
//
// This should only be used when the VoiceServerUpdate will be intercepted and used elsewhere.
//
//	gID     : Guild ID of the channel to join.
//	cID     : Channel ID of the channel to join, leave empty to disconnect.
//	mute    : If true, you will be set to muted upon joining.
//	deaf    : If true, you will be set to deafened upon joining.
func (s *Session) ChannelVoiceJoinManual(gID, cID string, mute, deaf bool) (err error) {

	s.log(LogDebug, "called")

	var channelID *string
	if cID == "" {
		channelID = nil
	} else {
		channelID = &cID
	}

	// Send the request to Discord that we want to join the voice channel
	data := voiceChannelJoinOp{4, voiceChannelJoinData{&gID, channelID, mute, deaf}}
	err = s.gatewayWrite(s.wsConn, data)
	return
}

// onVoiceStateUpdate handles Voice State Update events on the data websocket.
func (s *Session) onVoiceStateUpdate(st *VoiceStateUpdate) {

	// If we don't have a connection for the channel, don't bother
	if st.ChannelID == "" {
		return
	}

	// Check if we have a voice connection to update
	s.RLock()
	voice, exists := s.VoiceConnections[st.GuildID]
	s.RUnlock()
	if !exists {
		return
	}

	// We only care about events that are about us.
	if s.State.User.ID != st.UserID {
		return
	}

	// Store the SessionID for later use.
	voice.Lock()
	voice.UserID = st.UserID
	voice.sessionID = st.SessionID
	voice.ChannelID = st.ChannelID
	voice.Unlock()
}

// onVoiceServerUpdate handles the Voice Server Update data websocket event.
//
// This is also fired if the Guild's voice region changes while connected
// to a voice channel.  In that case, need to re-establish connection to
// the new region endpoint.
func (s *Session) onVoiceServerUpdate(st *VoiceServerUpdate) {

	s.log(LogDebug, "called")

	s.RLock()
	voice, exists := s.VoiceConnections[st.GuildID]
	s.RUnlock()

	// If no VoiceConnection exists, just skip this
	if !exists {
		return
	}

	// If currently connected to voice ws/udp, then disconnect.
	// Has no effect if not connected.
	voice.Close()

	// Store values for later use
	voice.Lock()
	voice.token = st.Token
	voice.endpoint = st.Endpoint
	voice.GuildID = st.GuildID
	voice.Unlock()

	// Open a connection to the voice server
	err := voice.open()
	if err != nil {
		s.log(LogError, "onVoiceServerUpdate voice.open, %s", err)
	}
}

type identifyOp struct {
	Op   int      `json:"op"`
	Data Identify `json:"d"`
}

// identify sends the identify packet to the gateway
func (s *Session) identify() error {
	s.log(LogDebug, "called")

	// TODO: This is a temporary block of code to help
	// maintain backwards compatibility
	if s.Compress == false {
		s.Identify.Compress = false
	}

	// TODO: This is a temporary block of code to help
	// maintain backwards compatibility
	if s.Token != "" && s.Identify.Token == "" {
		s.Identify.Token = s.Token
	}

	// TODO: Below block should be refactored so ShardID and ShardCount
	// can be deprecated and their usage moved to the Session.Identify
	// struct
	if s.ShardCount > 1 {

		if s.ShardID >= s.ShardCount {
			return ErrWSShardBounds
		}

		s.Identify.Shard = &[2]int{s.ShardID, s.ShardCount}
	}

	// Send Identify packet to Discord
	op := identifyOp{2, s.Identify}
	s.log(LogDebug, "sending gateway Identify")
	err := s.gatewayWrite(s.wsConn, op)

	return err
}

func (s *Session) reconnect(ctx context.Context) {
	// Teardown's socket identity check elects exactly one owner per failed
	// connection. A global retry flag would lose an immediate failure of the
	// replacement socket while this owner is still returning from Open.
	if !s.ShouldReconnectOnError {
		return
	}
	for delay := time.Second; ctx.Err() == nil; {
		s.log(LogInformational, "retrying gateway connection")
		err := s.openGateway(ctx)
		if err == ErrWSAlreadyOpen {
			return
		}
		if err == nil {
			if s.ShouldReconnectVoiceOnSessionError {
				s.RLock()
				voices := make([]*VoiceConnection, 0, len(s.VoiceConnections))
				for _, voice := range s.VoiceConnections {
					voices = append(voices, voice)
				}
				s.RUnlock()
				for _, voice := range voices {
					go voice.reconnect()
					if !waitGatewayRetry(ctx, time.Second) {
						return
					}
				}
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		s.log(LogWarning, "gateway reconnect failed; retry in %s: %s", delay, err)
		if !waitGatewayRetry(ctx, delay) {
			return
		}
		delay *= 2
		if delay > 10*time.Minute {
			delay = 10 * time.Minute
		}
	}
}

// Close closes the gateway and cancels pending reconnect attempts.
func (s *Session) Close() error { return s.CloseWithCode(websocket.CloseNormalClosure) }

// CloseWithCode closes the current gateway and cancels reconnect attempts.
// An explicit subsequent Open starts a new connection lifetime.
func (s *Session) CloseWithCode(closeCode int) error {
	s.gatewayMu.Lock()
	if s.gatewayCancel != nil {
		s.gatewayCancel()
	}
	s.gatewayHealth.Ready = false
	conn := s.gatewayConn
	s.gatewayConn = nil
	s.gatewayMu.Unlock()
	// Do not wait for Session.Lock to interrupt a blocked Open/write.
	if conn != nil {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(closeCode, ""), time.Now().Add(time.Second))
		_ = conn.Close()
	}
	s.Lock()
	closed := conn != nil && s.wsConn == conn
	err := s.closeGatewayLocked(conn, closeCode)
	s.Unlock()
	if closed {
		s.handleEvent(disconnectEventType, &Disconnect{})
	}
	return err
}
