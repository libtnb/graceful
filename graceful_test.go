package graceful

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// recorder collects stop order across components.
type recorder struct {
	mu    sync.Mutex
	order []string
}

func (r *recorder) stop(name string) func(context.Context) error {
	return func(context.Context) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.order = append(r.order, name)
		return nil
	}
}

// blockUntilStopped is a start func that blocks until its stop releases it.
type blocker struct{ done chan struct{} }

func newBlocker() *blocker { return &blocker{done: make(chan struct{})} }

func (b *blocker) start() error { <-b.done; return nil }

func (b *blocker) stop(context.Context) error { close(b.done); return nil }

func TestRun_FirstFailureStopsAll(t *testing.T) {
	rec := &recorder{}
	b := newBlocker()

	g := New()
	g.Add("steady", b.start, func(ctx context.Context) error {
		_ = b.stop(ctx)
		return rec.stop("steady")(ctx)
	})
	g.Add("broken", func() error { return errors.New("boom") }, rec.stop("broken"))

	err := g.Run()

	if err == nil || err.Error() != "broken: boom" {
		t.Fatalf("Run should surface the failing component, got %v", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.order) != 2 || rec.order[0] != "broken" || rec.order[1] != "steady" {
		t.Fatalf("all components should stop in reverse order, got %v", rec.order)
	}
}

func TestRun_SignalDrainsInReverseOrder(t *testing.T) {
	rec := &recorder{}
	b1, b2 := newBlocker(), newBlocker()

	g := New(WithShutdownTimeout(5 * time.Second))
	g.Add("first", b1.start, func(ctx context.Context) error {
		_ = b1.stop(ctx)
		return rec.stop("first")(ctx)
	})
	g.Add("second", b2.start, func(ctx context.Context) error {
		_ = b2.stop(ctx)
		return rec.stop("second")(ctx)
	})

	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	}()

	if err := g.Run(); err != nil {
		t.Fatalf("signal shutdown should return nil, got %v", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.order) != 2 || rec.order[0] != "second" || rec.order[1] != "first" {
		t.Fatalf("stop order should be reverse of registration, got %v", rec.order)
	}
}

func TestListen_ServesAndShutsDown(t *testing.T) {
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})}

	g := New()
	g.Listen("http", "127.0.0.1:0", srv)

	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	}()

	if err := g.Run(); err != nil {
		t.Fatalf("clean listener shutdown should return nil, got %v", err)
	}
}

func TestListen_BadAddressDrainsStarted(t *testing.T) {
	rec := &recorder{}
	b := newBlocker()

	g := New()
	g.Add("steady", b.start, func(ctx context.Context) error {
		_ = b.stop(ctx)
		return rec.stop("steady")(ctx)
	})
	g.Listen("http", "256.256.256.256:0", &http.Server{})

	err := g.Run()

	if err == nil {
		t.Fatal("Run should fail on an unbindable address")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.order) != 1 || rec.order[0] != "steady" {
		t.Fatalf("already-started components should be drained, got %v", rec.order)
	}
}

func TestServerFailurePropagates(t *testing.T) {
	// grab a port and hold it so the group's listener can't bind... instead,
	// simpler: a Server whose Serve fails immediately.
	g := New()
	g.Listen("http", "127.0.0.1:0", failingServer{})

	err := g.Run()

	if err == nil || !errors.Is(err, errServe) {
		t.Fatalf("serve failure should surface from Run, got %v", err)
	}
}

var errServe = errors.New("serve exploded")

type failingServer struct{}

func (failingServer) Serve(ln net.Listener) error { _ = ln.Close(); return errServe }

func (failingServer) Shutdown(context.Context) error { return nil }

func TestRun_WithUpgrade(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("tableflip upgrades are not supported on windows")
	}

	g := New(WithUpgrade())
	g.Listen("http", "127.0.0.1:0", &http.Server{})

	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	}()

	if err := g.Run(); err != nil {
		t.Fatalf("upgrade-enabled group should shut down cleanly, got %v", err)
	}
}
