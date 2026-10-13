package config

import (
	"strings"
	"testing"
)

const valid = `{"version":"v1","destinations":[{"id":"crm","adapter":"crm.contact.v1","url":"https://crm.example.com"}],"routes":[{"source":"billing","event_type":"subscription.paid","schema_version":1,"destinations":["crm","crm"]}],"credentials":[{"token_ref":"SOURCE_TOKEN","source":"billing"}]}`

func TestConfiguration(t *testing.T) {
	c, e := Decode(strings.NewReader(valid))
	if e != nil {
		t.Fatal(e)
	}
	if c.Destinations[0].TimeoutMS != 10000 {
		t.Fatal("defaults missing")
	}
	if len(c.Targets("billing", "subscription.paid", 1, "")) != 1 {
		t.Fatal("duplicate destination")
	}
	if len(c.Targets("unknown", "subscription.paid", 1, "")) != 0 {
		t.Fatal("source mismatch ignored")
	}
}
func TestRejectUnsafeConfiguration(t *testing.T) {
	for _, s := range []string{
		strings.Replace(valid, "https://crm.example.com", "https://user:password@crm.example.com", 1),
		strings.Replace(valid, "https://crm.example.com", "https://crm.example.com?token=secret", 1),
		strings.Replace(valid, "https://crm.example.com", "http://crm.example.com", 1),
		strings.Replace(valid, "\"url\"", "\"unknown\":true,\"url\"", 1),
		strings.Replace(valid, "\"adapter\":\"crm.contact.v1\"", "\"adapter\":\"unregistered\"", 1),
		strings.Replace(valid, "\"url\"", "\"headers\":{\"Authorization\":\"secret\"},\"url\"", 1),
		valid + `{}`,
	} {
		if _, e := Decode(strings.NewReader(s)); e == nil {
			t.Fatalf("accepted invalid config: %s", s)
		}
	}
}
