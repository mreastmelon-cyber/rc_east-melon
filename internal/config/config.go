package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
)

type Route struct {
	Source        string   `json:"source"`
	EventType     string   `json:"event_type"`
	SchemaVersion uint32   `json:"schema_version"`
	Channel       string   `json:"channel,omitempty"`
	Destinations  []string `json:"destinations"`
}
type Credential struct {
	TokenRef string `json:"token_ref"`
	Source   string `json:"source,omitempty"`
	Operator string `json:"operator,omitempty"`
	Admin    bool   `json:"admin,omitempty"`
}
type Config struct {
	Version      string               `json:"version"`
	Destinations []domain.Destination `json:"destinations"`
	Routes       []Route              `json:"routes"`
	Credentials  []Credential         `json:"credentials"`
}

func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	return Decode(f)
}
func Decode(r io.Reader) (Config, error) {
	var c Config
	d := json.NewDecoder(io.LimitReader(r, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("configuration must contain one JSON document")
	}
	return c, c.Validate()
}
func Sensitive(s string) bool {
	s = strings.ToLower(s)
	for _, v := range []string{"authorization", "cookie", "token", "secret", "password", "signature", "api-key", "api_key", "apikey"} {
		if strings.Contains(s, v) {
			return true
		}
	}
	return false
}
func (c *Config) Validate() error {
	if !domain.Identifier(c.Version, 64) || len(c.Destinations) == 0 || len(c.Routes) == 0 {
		return fmt.Errorf("version, destinations and routes required")
	}
	ids := map[string]bool{}
	for i := range c.Destinations {
		d := &c.Destinations[i]
		if !domain.Identifier(d.ID, 64) || ids[d.ID] {
			return fmt.Errorf("invalid or duplicate destination")
		}
		ids[d.ID] = true
		switch d.Adapter {
		case "ad.registration.v1", "crm.contact.v1", "inventory.purchase.v1":
		default:
			return fmt.Errorf("unregistered adapter")
		}
		u, e := url.Parse(d.URL)
		if e != nil || u.Host == "" || u.User != nil || u.Fragment != "" || len(d.URL) > 2048 || !(u.Scheme == "https" || u.Scheme == "http" && d.AllowHTTP) {
			return fmt.Errorf("invalid destination URL")
		}
		for k := range u.Query() {
			if Sensitive(k) {
				return fmt.Errorf("secret must be referenced, not embedded in URL")
			}
		}
		for k, v := range d.Headers {
			if Sensitive(k) || strings.EqualFold(k, "content-type") || strings.EqualFold(k, "host") || !validHeader(k) || strings.ContainsAny(k+v, "\r\n") {
				return fmt.Errorf("invalid static header")
			}
		}
		switch d.Auth.Type {
		case "", "none":
			d.Auth.Type = "none"
		case "bearer", "basic", "api_key", "hmac":
			if !domain.Identifier(d.Auth.SecretRef, 128) {
				return fmt.Errorf("secret reference required")
			}
		default:
			return fmt.Errorf("unsupported auth")
		}
		if d.Auth.Type == "api_key" && (!validHeader(d.Auth.Header)) {
			return fmt.Errorf("API key header required")
		}
		if d.TimeoutMS == 0 {
			d.TimeoutMS = 10000
		}
		if d.MaxAttempts == 0 {
			d.MaxAttempts = 12
		}
		if d.DeadlineSeconds == 0 {
			d.DeadlineSeconds = 86400
		}
		if d.Retry.BaseMS == 0 {
			d.Retry.BaseMS = 5000
		}
		if d.Retry.CapMS == 0 {
			d.Retry.CapMS = 1800000
		}
		if d.TimeoutMS < 1 || d.TimeoutMS > 30000 || d.MaxAttempts > 100 || d.DeadlineSeconds < 1 || d.DeadlineSeconds > 604800 || d.Retry.BaseMS < 1 || d.Retry.CapMS < d.Retry.BaseMS || d.Retry.CapMS > 86400000 {
			return fmt.Errorf("invalid execution limits")
		}
		if d.IdempotencyMode == "" {
			d.IdempotencyMode = "none"
		}
		switch d.IdempotencyMode {
		case "none", "natural":
			if d.IdempotencyRetentionSeconds != 0 {
				return fmt.Errorf("unexpected idempotency retention")
			}
		case "provider_key":
			if !validHeader(d.IdempotencyHeader) || Sensitive(d.IdempotencyHeader) || strings.EqualFold(d.IdempotencyHeader, "Content-Type") || strings.EqualFold(d.IdempotencyHeader, "Host") || strings.EqualFold(d.IdempotencyHeader, "X-Timestamp") || strings.EqualFold(d.IdempotencyHeader, d.Auth.Header) || d.IdempotencyRetentionSeconds == 0 {
				return fmt.Errorf("provider key contract required")
			}
		default:
			return fmt.Errorf("invalid idempotency mode")
		}
		if len(d.Success.Statuses) == 0 {
			d.Success.Statuses = []int{200, 201, 204}
		}
		for _, s := range d.Success.Statuses {
			if s < 200 || s > 299 {
				return fmt.Errorf("success status must be 2xx")
			}
		}
	}
	for _, r := range c.Routes {
		if !domain.Identifier(r.Source, 64) || r.SchemaVersion != 1 || len(r.Destinations) == 0 {
			return fmt.Errorf("invalid route")
		}
		switch r.EventType {
		case "user.registered", "subscription.paid", "purchase.completed":
		default:
			return fmt.Errorf("unsupported event type")
		}
		for _, id := range r.Destinations {
			if !ids[id] {
				return fmt.Errorf("route references missing destination")
			}
		}
	}
	refs := map[string]bool{}
	for _, a := range c.Credentials {
		if !domain.Identifier(a.TokenRef, 128) || refs[a.TokenRef] {
			return fmt.Errorf("invalid credential reference")
		}
		refs[a.TokenRef] = true
		if a.Admin {
			if !domain.Identifier(a.Operator, 128) || a.Source != "" {
				return fmt.Errorf("invalid management identity")
			}
		} else if !domain.Identifier(a.Source, 64) || a.Operator != "" {
			return fmt.Errorf("invalid source identity")
		}
	}
	return nil
}
func (c Config) Destination(id string) (domain.Destination, bool) {
	for _, d := range c.Destinations {
		if d.ID == id {
			return d, true
		}
	}
	return domain.Destination{}, false
}
func (c Config) Targets(source, kind string, version uint32, channel string) []domain.Destination {
	seen := map[string]bool{}
	var out []domain.Destination
	for _, r := range c.Routes {
		if r.Source != source || r.EventType != kind || r.SchemaVersion != version || r.Channel != "" && r.Channel != channel {
			continue
		}
		for _, id := range r.Destinations {
			if !seen[id] {
				d, _ := c.Destination(id)
				out = append(out, d)
				seen[id] = true
			}
		}
	}
	return out
}

func validHeader(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c) {
			continue
		}
		return false
	}
	return true
}
