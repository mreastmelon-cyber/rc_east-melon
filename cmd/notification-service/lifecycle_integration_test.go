//go:build integration

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mreastmelon-cyber/right-capital-project/internal/domain"
	"github.com/mreastmelon-cyber/right-capital-project/internal/repository"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}
func TestServiceStartupHealthAndShutdown(t *testing.T) {
	raw := os.Getenv("MYSQL_TEST_DSN")
	if raw == "" {
		t.Fatal("MYSQL_TEST_DSN is required")
	}
	cfg, e := mysql.ParseDSN(raw)
	if e != nil {
		t.Fatal("invalid test DSN")
	}
	host, _, e := net.SplitHostPort(cfg.Addr)
	if e != nil || cfg.Net != "tcp" || (host != "127.0.0.1" && host != "localhost" && host != "::1") {
		t.Fatal("test database must be loopback")
	}
	cfg.DBName = ""
	admin, e := repository.Open(context.Background(), cfg.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	defer admin.DB.Close()
	name := "notification_it_" + strings.ReplaceAll(domain.UUID(), "-", "")
	if _, e = admin.DB.Exec("CREATE DATABASE `" + name + "`"); e != nil {
		t.Fatal(e)
	}
	defer admin.DB.Exec("DROP DATABASE `" + name + "`")
	cfg.DBName = name
	t.Setenv("MYSQL_DSN", cfg.FormatDSN())
	for _, ref := range []string{"IDENTITY_RPC_TOKEN", "BILLING_RPC_TOKEN", "SHOP_RPC_TOKEN", "ADMIN_RPC_TOKEN"} {
		t.Setenv(ref, ref+"-local-test")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if e = run(context.Background(), []string{"-mode", "migrate"}, logger); e != nil {
		t.Fatal(e)
	}
	if e = run(context.Background(), []string{"-mode", "api", "-config", "../../configs/example.json"}, logger); e == nil || !strings.Contains(e.Error(), "TLS") {
		t.Fatal("production plaintext permitted", e)
	}
	for _, plaintext := range []bool{true, false} {
		t.Run(map[bool]string{true: "plaintext-development", false: "TLS"}[plaintext], func(t *testing.T) {
			rpcAddr, metricsAddr := freeAddress(t), freeAddress(t)
			args := []string{"-config", "../../configs/example.json", "-listen", rpcAddr, "-metrics-listen", metricsAddr}
			var ca string
			if plaintext {
				args = append(args, "-insecure-dev")
			} else {
				cert, key := testCertificate(t)
				ca = cert
				args = append(args, "-tls-cert", cert, "-tls-key", key)
			}
			parent, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- run(parent, args, logger) }()
			defer func() {
				cancel()
				select {
				case e := <-done:
					if e != nil {
						t.Error(e)
					}
				case <-time.After(30 * time.Second):
					t.Error("shutdown did not finish")
				}
			}()
			until := time.Now().Add(10 * time.Second)
			for {
				ctx, stop := context.WithTimeout(context.Background(), 250*time.Millisecond)
				err := checkHealth(ctx, rpcAddr, plaintext, ca)
				stop()
				if err == nil {
					break
				}
				if time.Now().After(until) {
					t.Fatal("service never ready", err)
				}
				time.Sleep(25 * time.Millisecond)
			}
			client := &http.Client{Timeout: time.Second}
			resp, e := client.Get("http://" + metricsAddr + "/metrics")
			if e != nil {
				t.Fatal(e)
			}
			data, e := io.ReadAll(resp.Body)
			resp.Body.Close()
			if e != nil || resp.StatusCode != 200 || !strings.Contains(string(data), "notification_worker_heartbeat_timestamp_seconds") {
				t.Fatal("metrics endpoint failed", e)
			}
			if !plaintext {
				if e = checkHealth(context.Background(), rpcAddr, false, ""); e == nil {
					t.Fatal("untrusted TLS certificate accepted")
				}
			}
		})
	}
}
func testCertificate(t *testing.T) (string, string) {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	secret, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if e = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: secret}), 0600); e != nil {
		t.Fatal(e)
	}
	return certPath, keyPath
}
