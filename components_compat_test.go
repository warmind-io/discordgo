package discordgo

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestMessageComponentCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
		kind ComponentType
	}{
		{"button", `{"type":2,"style":1,"label":"OK","custom_id":"ok","disabled":false}`, ButtonComponent},
		{"container", `{"type":17,"accent_color":123,"components":[{"type":10,"content":"hello"}]}`, 17},
		{"future", `{"type":99,"future":{"items":[1,true,null]}}`, 99},
		{"nested in row", `{"type":1,"components":[{"type":99,"custom":"keep"}]}`, ActionsRowComponent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(tc.data)
			component, err := MessageComponentFromJSON(data)
			if err != nil {
				t.Fatal(err)
			}
			if component.Type() != tc.kind {
				t.Fatalf("component type = %d, want %d", component.Type(), tc.kind)
			}
			if tc.kind == ButtonComponent {
				if _, ok := component.(*Button); !ok {
					t.Fatalf("known component lost its typed representation: %T", component)
				}
			}
			// The event reader can reuse its input after decoding.
			for i := range data {
				data[i] = ' '
			}
			encoded, err := json.Marshal(component)
			if err != nil {
				t.Fatal(err)
			}
			var before, after interface{}
			if err := json.Unmarshal([]byte(tc.data), &before); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("component changed on round trip: %s", encoded)
			}
		})
	}
}

func TestMessageComponentRejectsMalformedPayload(t *testing.T) {
	for _, data := range []string{`null`, `{}`, `{"type":0}`, `{"type":-1}`, `{"type":"17"}`, `{"type":17,`, `{"type":2,"style":"bad"}`} {
		t.Run(data, func(t *testing.T) {
			if _, err := MessageComponentFromJSON([]byte(data)); err == nil {
				t.Fatal("malformed component was accepted")
			}
		})
	}
}

func TestGatewayMessageCreateWithUnknownComponents(t *testing.T) {
	for _, components := range []string{
		`[{"type":17,"components":[{"type":10,"content":"hello"}]}]`,
		`[{"type":1,"components":[{"type":99,"future":true}]}]`,
	} {
		t.Run(components, func(t *testing.T) {
			s, err := New("")
			if err != nil {
				t.Fatal(err)
			}
			s.StateEnabled = false
			s.SyncEvents = true
			called := false
			s.AddHandler(func(_ *Session, e *MessageCreate) {
				called = true
				if e.Message == nil || e.Author == nil || e.Author.ID != "4" || !e.Author.Bot {
					t.Fatalf("message author lost during decoding: %+v", e.Message)
				}
				if e.ID != "1" || e.ChannelID != "2" || e.GuildID != "3" || e.Content != "outer" {
					t.Fatalf("core message fields lost: %+v", e.Message)
				}
				if e.ReferencedMessage == nil || e.ReferencedMessage.Author == nil || e.ReferencedMessage.Content != "inner" {
					t.Fatal("referenced message lost during decoding")
				}
				if len(e.Components) != 1 || len(e.ReferencedMessage.Components) != 1 {
					t.Fatal("components were discarded")
				}
			})
			// Exercise the actual gateway decoder/dispatch path without a socket.
			packet := fmt.Sprintf(`{"op":0,"s":1,"t":"MESSAGE_CREATE","d":{"id":"1","channel_id":"2","guild_id":"3","content":"outer","author":{"id":"4","bot":true},"components":%s,"referenced_message":{"id":"5","content":"inner","author":{"id":"6"},"components":%s}}}`, components, components)
			if _, err := s.onEvent(1, []byte(packet)); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("message handler was not called")
			}
		})
	}
}
