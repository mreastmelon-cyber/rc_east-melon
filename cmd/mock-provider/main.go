package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:8081", "mock supplier address")
	flag.Parse()
	var mu sync.Mutex
	keys := map[string]bool{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /{kind}", func(w http.ResponseWriter, r *http.Request) {
		body, e := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
		if e != nil || len(body) > 1<<20 {
			http.Error(w, "invalid request", 400)
			return
		}
		switch r.PathValue("kind") {
		case "ad":
			form, err := url.ParseQuery(string(body))
			if err != nil || form.Get("user_id") == "" {
				http.Error(w, "invalid registration", 400)
				return
			}
		case "crm":
			var data map[string]string
			if json.Unmarshal(body, &data) != nil || data["customer_id"] == "" || data["status"] != "paid" {
				http.Error(w, "invalid payment", 400)
				return
			}
		case "inventory":
			var data struct {
				OrderID   string `json:"order_id"`
				ProductID string `json:"product_id"`
				Quantity  uint32 `json:"quantity"`
			}
			if json.Unmarshal(body, &data) != nil || data.OrderID == "" || data.ProductID == "" || data.Quantity == 0 || r.Header.Get("Idempotency-Key") == "" {
				http.Error(w, "invalid purchase", 400)
				return
			}
		default:
			http.NotFound(w, r)
			return
		}
		key := r.Header.Get("Idempotency-Key")
		mu.Lock()
		duplicate := key != "" && keys[key]
		if key != "" {
			keys[key] = true
		}
		mu.Unlock()
		slog.Info("mock notification accepted", "method", r.Method, "path", r.URL.Path, "idempotency_key", key, "body_sha256", fmt.Sprintf("%x", sha256.Sum256(body)), "duplicate", duplicate, "http_status", http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"success": true, "duplicate": duplicate})
	})
	server := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() {
		slog.Info("mock provider started", "address", *addr)
		if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			slog.Error("mock provider failed")
			cancel()
		}
	}()
	<-ctx.Done()
	stop, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	server.Shutdown(stop)
}
