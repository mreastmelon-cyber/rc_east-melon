package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mreastmelon-cyber/right-capital-project/internal/app"
	"github.com/mreastmelon-cyber/right-capital-project/internal/config"
	"github.com/mreastmelon-cyber/right-capital-project/internal/observability"
	"github.com/mreastmelon-cyber/right-capital-project/internal/repository"
	serviceRPC "github.com/mreastmelon-cyber/right-capital-project/internal/rpc"
	"github.com/mreastmelon-cyber/right-capital-project/internal/transport"
	"github.com/mreastmelon-cyber/right-capital-project/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcinsecure "google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
func run(parent context.Context, args []string, logger *slog.Logger) error {
	flags := flag.NewFlagSet("notification-service", flag.ContinueOnError)
	mode := flags.String("mode", "all", "all, api, worker, migrate or healthcheck")
	path := flags.String("config", "configs/example.json", "versioned configuration file")
	addr := flags.String("listen", "127.0.0.1:50051", "gRPC listen address")
	metricsAddr := flags.String("metrics-listen", "127.0.0.1:9090", "internal Prometheus address")
	insecure := flags.Bool("insecure-dev", false, "enable plaintext gRPC for local development")
	reflection := flags.Bool("reflection", false, "enable authenticated gRPC reflection")
	cert := flags.String("tls-cert", "", "TLS certificate path")
	key := flags.String("tls-key", "", "TLS private key path")
	ca := flags.String("tls-ca", "", "trusted CA bundle for TLS healthcheck")
	migrate := flags.Bool("migrate", false, "apply migrations before starting service")
	count := flags.Int("workers", 8, "fixed task execution pool size, 1..32")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || (*mode != "all" && *mode != "api" && *mode != "worker" && *mode != "migrate" && *mode != "healthcheck") || *count < 1 || *count > 32 {
		return errors.New("invalid service options")
	}
	if *mode == "healthcheck" {
		return checkHealth(parent, *addr, *insecure, *ca)
	}
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return errors.New("MYSQL_DSN is required")
	}
	setup, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	store, err := repository.Open(setup, dsn)
	if err != nil {
		return errors.New("database connection failed; check MYSQL_DSN and server availability")
	}
	defer store.DB.Close()
	if *migrate || *mode == "migrate" {
		if err = store.Migrate(setup); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}
	if *mode == "migrate" {
		logger.Info("database migrations complete")
		return nil
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	service := &app.Service{Store: store, Config: cfg}
	metrics := observability.New()
	sender := transport.New(os.LookupEnv)
	defer sender.Client.CloseIdleConnections()
	runner := &worker.Runner{Store: store, Config: cfg, Sender: sender, Metrics: metrics, Logger: logger, MaxWorkers: *count}
	life, stop := context.WithCancel(context.WithoutCancel(parent))
	defer stop()
	workerDone := make(chan error, 1)
	if *mode == "all" || *mode == "worker" {
		go func() { workerDone <- runner.Run(life) }()
	}
	var rpcServer *grpc.Server
	var setHealth func(bool)
	errorsCh := make(chan error, 2)
	if *mode == "all" || *mode == "api" {
		if err = serviceRPC.ValidateSecrets(cfg, os.LookupEnv); err != nil {
			stop()
			if *mode == "all" {
				<-workerDone
			}
			return errors.New("RPC credential references must resolve to distinct nonempty secrets")
		}
		var options []grpc.ServerOption
		if !*insecure {
			if *cert == "" || *key == "" {
				stop()
				if *mode == "all" {
					<-workerDone
				}
				return errors.New("TLS certificate and key required, or use -insecure-dev locally")
			}
			pair, e := tls.LoadX509KeyPair(*cert, *key)
			if e != nil {
				stop()
				if *mode == "all" {
					<-workerDone
				}
				return errors.New("could not load TLS certificate and key")
			}
			options = append(options, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12})))
		}
		options = append(options, grpc.ChainUnaryInterceptor(metrics.Interceptor(logger)))
		server, health := serviceRPC.New(service, os.LookupEnv, *reflection, options...)
		rpcServer = server
		setHealth = func(ready bool) {
			state := healthpb.HealthCheckResponse_NOT_SERVING
			if ready {
				state = healthpb.HealthCheckResponse_SERVING
			}
			health.SetServingStatus("", state)
		}
		listener, e := net.Listen("tcp", *addr)
		if e != nil {
			stop()
			if *mode == "all" {
				<-workerDone
			}
			return errors.New("gRPC listen failed")
		}
		go func() {
			e := server.Serve(listener)
			if e != nil && !errors.Is(e, grpc.ErrServerStopped) {
				errorsCh <- errors.New("gRPC server failed")
			}
		}()
		logger.Info("gRPC server started", "address", listener.Addr().String(), "mode", *mode)
	}
	httpServer := &http.Server{Addr: *metricsAddr, Handler: metrics.Handler(), ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		if e := httpServer.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
			errorsCh <- errors.New("metrics server failed")
		}
	}()
	healthDone := make(chan struct{})
	go func() {
		defer close(healthDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			ctx, cancel := context.WithTimeout(life, time.Second)
			ready := store.DB.PingContext(ctx) == nil
			cancel()
			if *mode != "api" {
				ready = ready && runner.Active.Load()
			}
			if setHealth != nil {
				setHealth(ready)
			}
			select {
			case <-life.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	var result error
	workerExited := false
	select {
	case <-parent.Done():
	case result = <-errorsCh:
	case result = <-workerDone:
		workerExited = true
	}
	stop()
	<-healthDone
	if setHealth != nil {
		setHealth(false)
	}
	if rpcServer != nil {
		done := make(chan struct{})
		go func() { rpcServer.GracefulStop(); close(done) }()
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-done:
			timer.Stop()
		case <-timer.C:
			rpcServer.Stop()
			<-done
		}
	}
	shutdown, cancelHTTP := context.WithTimeout(context.Background(), 5*time.Second)
	httpServer.Shutdown(shutdown)
	cancelHTTP()
	if (*mode == "all" || *mode == "worker") && !workerExited {
		if e := <-workerDone; result == nil {
			result = e
		}
	}
	logger.Info("service shutdown complete")
	return result
}

func checkHealth(parent context.Context, addr string, plaintext bool, caPath string) error {
	var creds credentials.TransportCredentials
	if plaintext {
		creds = grpcinsecure.NewCredentials()
	} else {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if caPath != "" {
			pem, err := os.ReadFile(caPath)
			if err != nil {
				return errors.New("could not read healthcheck CA")
			}
			cfg.RootCAs = x509.NewCertPool()
			if !cfg.RootCAs.AppendCertsFromPEM(pem) {
				return errors.New("invalid healthcheck CA")
			}
		}
		creds = credentials.NewTLS(cfg)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return errors.New("healthcheck client failed")
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	reply, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || reply.Status != healthpb.HealthCheckResponse_SERVING {
		return errors.New("service is not ready")
	}
	return nil
}
