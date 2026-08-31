package graceful

import (
	"context"
	"errors"
	"net"
	"net/http"
	"runtime"
	"sync"
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

func (r *recorder) stopped() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

// blocker is a start func that blocks until its stop releases it.
type blocker struct{ done chan struct{} }

func newBlocker() *blocker { return &blocker{done: make(chan struct{})} }

func (b *blocker) start() error { <-b.done; return nil }

func (b *blocker) stop(context.Context) error { close(b.done); return nil }

// cancelSoon returns a context that cancels itself shortly after Run starts.
func cancelSoon(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	return ctx
}

func TestRun_FirstFailureStopsAll(t *testing.T) {
	rec := &recorder{}
	b := newBlocker()

	g := New()
	g.Add("steady", b.start, func(ctx context.Context) error {
		_ = b.stop(ctx)
		return rec.stop("steady")(ctx)
	})
	g.Add("broken", func() error { return errors.New("boom") }, rec.stop("broken"))

	err := g.Run(context.Background())

	if err == nil || err.Error() != "broken: boom" {
		t.Fatalf("Run should surface the failing component, got %v", err)
	}
	if order := rec.stopped(); len(order) != 2 || order[0] != "broken" || order[1] != "steady" {
		t.Fatalf("all components should stop in reverse order, got %v", order)
	}
}

func TestRun_CancelDrainsInReverseOrder(t *testing.T) {
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

	if err := g.Run(cancelSoon(t)); err != nil {
		t.Fatalf("requested shutdown should return nil, got %v", err)
	}
	if order := rec.stopped(); len(order) != 2 || order[0] != "second" || order[1] != "first" {
		t.Fatalf("stop order should be reverse of registration, got %v", order)
	}
}

func TestRun_DrainFailureIsReturned(t *testing.T) {
	errStop := errors.New("stop exploded")
	b := newBlocker()

	g := New()
	g.Add("grumpy", b.start, func(ctx context.Context) error {
		_ = b.stop(ctx)
		return errStop
	})

	if err := g.Run(cancelSoon(t)); !errors.Is(err, errStop) {
		t.Fatalf("drain failures should surface from Run, got %v", err)
	}
}

func TestListen_ServesAndShutsDown(t *testing.T) {
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})}

	g := New()
	g.Listen("http", "127.0.0.1:0", srv)

	if err := g.Run(cancelSoon(t)); err != nil {
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

	err := g.Run(context.Background())

	if err == nil {
		t.Fatal("Run should fail on an unbindable address")
	}
	if order := rec.stopped(); len(order) != 1 || order[0] != "steady" {
		t.Fatalf("already-started components should be drained, got %v", order)
	}
}

func TestServerFailurePropagates(t *testing.T) {
	g := New()
	g.Listen("http", "127.0.0.1:0", failingServer{})

	err := g.Run(context.Background())

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

	if err := g.Run(cancelSoon(t)); err != nil {
		t.Fatalf("upgrade-enabled group should shut down cleanly, got %v", err)
	}
}
