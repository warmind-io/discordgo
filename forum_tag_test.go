package discordgo

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestForumTagIDCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, value, want string
	}{
		{"string", `"725062952465727539"`, "725062952465727539"},
		{"number", `725062952465727539`, "725062952465727539"},
		{"above float precision", `9007199254740993`, "9007199254740993"},
		{"max uint64", `18446744073709551615`, "18446744073709551615"},
		{"zero", `0`, "0"},
		{"empty string", `""`, ""},
		{"leading zero string", `"000123"`, "000123"},
		{"opaque string", `"existing-id"`, "existing-id"},
		{"escaped string", `"\u0031\u0032\u0033"`, "123"},
		{"null", `null`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got ForumTag
			data := `{"id":` + tc.value + `,"name":"LFG","moderated":true,"emoji_id":"123","emoji_name":"party"}`
			if err := json.Unmarshal([]byte(data), &got); err != nil {
				t.Fatal(err)
			}
			want := ForumTag{ID: tc.want, Name: "LFG", Moderated: true, EmojiID: "123", EmojiName: "party"}
			if got != want {
				t.Fatalf("decoded %+v, want %+v", got, want)
			}
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			if tc.want == "" {
				if _, ok := fields["id"]; ok {
					t.Fatalf("empty ID must remain omitted: %s", encoded)
				}
			} else if quoted, _ := json.Marshal(tc.want); string(fields["id"]) != string(quoted) {
				t.Fatalf("ID must marshal as an exact string: %s", encoded)
			}
		})
	}
}

func TestForumTagIDMissingNullAndDuplicate(t *testing.T) {
	for _, tc := range []struct{ data, want string }{
		{`{}`, "old"},
		{`null`, "old"},
		{`{"id":null}`, "old"},
		{`{"id":"new","id":null}`, "new"},
		{`{"id":9007199254740993,"id":null}`, "9007199254740993"},
		{`{"id":1,"id":"002"}`, "002"},
		{`{"id":"001","id":2}`, "2"},
	} {
		t.Run(tc.data, func(t *testing.T) {
			got := ForumTag{ID: "old", Name: "keep", Moderated: true, EmojiID: "123", EmojiName: "keep"}
			want := got
			want.ID = tc.want
			if err := json.Unmarshal([]byte(tc.data), &got); err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("decoded %+v, want %+v", got, want)
			}
		})
	}
}

func TestForumTagIDRejectsMalformed(t *testing.T) {
	for _, value := range []string{`-1`, `-0`, `1.5`, `1.0`, `1e3`, `18446744073709551616`, `true`, `false`, `[]`, `{}`, `01`, `+1`, `NaN`, `"unterminated`} {
		t.Run(value, func(t *testing.T) {
			got := ForumTag{ID: "old", Name: "keep"}
			before := got
			if err := json.Unmarshal([]byte(`{"name":"replace","id":`+value+`}`), &got); err == nil {
				t.Fatal("malformed ID accepted")
			}
			if got != before {
				t.Fatalf("failed decode changed receiver: %+v", got)
			}
		})
	}
}

func TestGuildChannelsForumTagIDs(t *testing.T) {
	const channel = `{"id":"123","type":15,"available_tags":[{"id":725062952465727539,"name":"numeric"},{"id":"9007199254740993","name":"string"},{"id":null,"name":"null"}]}`
	want := []ForumTag{{ID: "725062952465727539", Name: "numeric"}, {ID: "9007199254740993", Name: "string"}, {Name: "null"}}
	s, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	s.Client = &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet || req.URL.String() != EndpointGuildChannels("725062952465727538") {
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioutil.NopCloser(strings.NewReader("[" + channel + "]")), Request: req}, nil
	})}
	channels, err := s.GuildChannels("725062952465727538")
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 1 || !reflect.DeepEqual(channels[0].AvailableTags, want) {
		t.Fatalf("unexpected REST channels: %+v", channels)
	}
	var create GuildCreate
	if err := json.Unmarshal([]byte(`{"id":"725062952465727538","channels":[`+channel+`]}`), &create); err != nil {
		t.Fatal(err)
	}
	if len(create.Channels) != 1 || !reflect.DeepEqual(create.Channels[0].AvailableTags, want) {
		t.Fatalf("unexpected GUILD_CREATE channels: %+v", create.Channels)
	}
}
