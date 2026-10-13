package rpc

import (
	"context"
	"testing"

	"github.com/mreastmelon-cyber/right-capital-project/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthenticationAndRoles(t *testing.T) {
	c := config.Config{Credentials: []config.Credential{{TokenRef: "SOURCE", Source: "billing"}, {TokenRef: "ADMIN", Operator: "operator-1", Admin: true}}}
	secrets := func(k string) (string, bool) {
		if k == "SOURCE" {
			return "source-token", true
		}
		if k == "ADMIN" {
			return "admin-token", true
		}
		return "", false
	}
	if ValidateSecrets(c, secrets) != nil {
		t.Fatal("valid credentials rejected")
	}
	cases := []struct {
		token, method string
		want          codes.Code
	}{{"", "/notification.v1.NotificationService/SubmitEvent", codes.Unauthenticated}, {"bad", "/notification.v1.NotificationService/SubmitEvent", codes.Unauthenticated}, {"source-token", "/notification.v1.NotificationService/SubmitEvent", codes.OK}, {"source-token", "/notification.v1.ManagementService/ReplayTask", codes.PermissionDenied}, {"admin-token", "/notification.v1.ManagementService/ReplayTask", codes.OK}, {"admin-token", "/notification.v1.NotificationService/SubmitEvent", codes.PermissionDenied}}
	for _, cse := range cases {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+cse.token))
		_, e := Authenticate(ctx, cse.method, c, secrets)
		if status.Code(e) != cse.want {
			t.Errorf("%s: %v", cse.method, e)
		}
	}
	if ValidateSecrets(c, func(string) (string, bool) { return "same", true }) == nil {
		t.Fatal("ambiguous credentials accepted")
	}
}
