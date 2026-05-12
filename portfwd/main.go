package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config holds all configuration for the port forwarder.
type Config struct {
	ListenAddr string
	ListenPort int
	RemoteAddr string
	RemotePort int
	BufferSize int
	Verbose    bool
}

// Forwarder manages the TCP port forwarding lifecycle.
type Forwarder struct {
	config Config
	logger *log.Logger
	wg     sync.WaitGroup
	quit   chan struct{}
}

// NewForwarder creates a new Forwarder with the given config.
func NewForwarder(config Config) *Forwarder {
	flags := log.Ldate | log.Ltime
	if config.Verbose {
		flags |= log.Lshortfile
	}
	return &Forwarder{
		config: config,
		logger: log.New(os.Stdout, "[portfwd] ", flags),
		quit:   make(chan struct{}),
	}
}

// Start begins listening and forwarding connections.
// It blocks until the listener is ready, then spawns the accept loop in a goroutine.
func (f *Forwarder) Start() error {
	listenAddr := fmt.Sprintf("%s:%d", f.config.ListenAddr, f.config.ListenPort)
	targetAddr := fmt.Sprintf("%s:%d", f.config.RemoteAddr, f.config.RemotePort)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	f.logf("Listening on %s → forwarding to %s (buffer: %d bytes)", listenAddr, targetAddr, f.config.BufferSize)

	f.wg.Add(1)
	go f.acceptLoop(listener, targetAddr)

	return nil
}

// acceptLoop accepts incoming connections and spawns a handler for each.
func (f *Forwarder) acceptLoop(listener net.Listener, targetAddr string) {
	defer f.wg.Done()
	defer listener.Close()

	for {
		select {
		case <-f.quit:
			f.logf("Shutting down listener on %s", listener.Addr().String())
			return
		default:
		}

		// Set accept deadline so we can check f.quit periodically.
		listener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))

		conn, err := listener.Accept()
		if err != nil {
			// Timeout or temporary — check if we're quitting.
			if netErr, ok := err.(net.Error); ok && (netErr.Timeout() || netErr.Temporary()) {
				continue
			}
			select {
			case <-f.quit:
				return
			default:
				f.logf("Accept error: %v", err)
				continue
			}
		}

		f.wg.Add(1)
		go f.handleConnection(conn, targetAddr)
	}
}

// handleConnection handles a single incoming connection by connecting to the
// remote target and piping data bidirectionally.
func (f *Forwarder) handleConnection(incoming net.Conn, targetAddr string) {
	defer f.wg.Done()
	defer incoming.Close()

	remoteConn, err := net.DialTimeout("tcp", targetAddr, 10*time.Second)
	if err != nil {
		f.logf("Failed to connect to target %s: %v", targetAddr, err)
		return
	}
	defer remoteConn.Close()

	srcAddr := incoming.RemoteAddr().String()
	f.logf("Connection from %s → %s established", srcAddr, targetAddr)

	// Bidirectional copy: both directions run simultaneously.
	var innerWg sync.WaitGroup
	innerWg.Add(2)

	go func() {
		defer innerWg.Done()
		f.pipe(incoming, remoteConn, srcAddr+"→target")
	}()
	go func() {
		defer innerWg.Done()
		f.pipe(remoteConn, incoming, "target→"+srcAddr)
	}()

	innerWg.Wait()
	f.logf("Connection from %s closed", srcAddr)
}

// pipe copies data from src to dst, logging progress in verbose mode.
func (f *Forwarder) pipe(dst io.Writer, src io.Reader, label string) {
	buf := make([]byte, f.config.BufferSize)
	written, err := io.CopyBuffer(dst, src, buf)
	if err != nil && err != io.EOF {
		// Ignore EOF and "use of closed network connection" during shutdown.
		if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
			return
		}
		f.logf("Pipe error (%s): %v", label, err)
	}
	if f.config.Verbose && written > 0 {
		f.logf("Pipe %s: %d bytes transferred", label, written)
	}
}

// Stop signals the forwarder to shut down gracefully.
func (f *Forwarder) Stop() {
	f.logf("Initiating graceful shutdown...")
	close(f.quit)
	f.wg.Wait()
	f.logf("Shutdown complete.")
}

// logf is a convenience wrapper for structured logging.
func (f *Forwarder) logf(format string, args ...interface{}) {
	f.logger.Printf(format, args...)
}

func main() {
	cfg := parseFlags()

	fwd := NewForwarder(cfg)

	if err := fwd.Start(); err != nil {
		log.Fatalf("Failed to start forwarder: %v", err)
	}

	// Wait for SIGINT or SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	fmt.Println() // newline after ^C
	fwd.logf("Received signal: %v", sig)

	fwd.Stop()
}

// parseFlags parses command-line flags into a Config struct.
func parseFlags() Config {
	var cfg Config

	flag.StringVar(&cfg.ListenAddr, "l", "0.0.0.0", "Local address to listen on")
	flag.IntVar(&cfg.ListenPort, "lp", 0, "Local port to listen on (required)")
	flag.StringVar(&cfg.RemoteAddr, "r", "127.0.0.1", "Remote target address")
	flag.IntVar(&cfg.RemotePort, "rp", 0, "Remote target port (required)")
	flag.IntVar(&cfg.BufferSize, "buf", 4096, "Transfer buffer size in bytes")
	flag.BoolVar(&cfg.Verbose, "v", false, "Enable verbose logging")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "PortFwd - TCP Port Forwarding Tool\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  portfwd -lp <local_port> -rp <remote_port> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  portfwd -lp 8080 -rp 80 -r 192.168.1.100\n")
		fmt.Fprintf(os.Stderr, "  portfwd -lp 4444 -rp 3389 -r 10.0.0.50 -v\n")
		fmt.Fprintf(os.Stderr, "  portfwd -l 127.0.0.1 -lp 1080 -rp 22 -r 10.0.0.1 -buf 8192\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if cfg.ListenPort == 0 || cfg.RemotePort == 0 {
		flag.Usage()
		os.Exit(1)
	}

	return cfg
}
