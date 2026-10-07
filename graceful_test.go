package graceful

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

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

func cancelSoon(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)
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

	err := g.Run(cancelSoon(t))
	if !errors.Is(err, errStop) {
		t.Fatalf("drain failures should surface from Run, got %v", err)
	}
	if _, ok := err.(*DrainError); !ok {
		t.Fatalf("a requested shutdown with drain failures should return a bare *DrainError, got %T", err)
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

func TestRun_ReportsToServiceManager(t *testing.T) {
	manager := notifySocket(t)
	// the test runner plays the manager that forked us
	t.Setenv("MANAGERPID", strconv.Itoa(os.Getppid()))

	g := New()
	g.Listen("http", "127.0.0.1:0", &http.Server{})

	if err := g.Run(cancelSoon(t)); err != nil {
		t.Fatalf("Run should shut down cleanly, got %v", err)
	}

	want := []string{"MAINPID=" + strconv.Itoa(os.Getpid()) + "\nREADY=1", "STOPPING=1"}
	for _, state := range want {
		if got := readState(t, manager); got != state {
			t.Fatalf("service manager should receive %q, got %q", state, got)
		}
	}
}

func TestRun_LeavesMainPIDToTheParentUntilAdopted(t *testing.T) {
	manager := notifySocket(t)
	// a manager that is not our parent, as for a process an upgrade spawned
	t.Setenv("MANAGERPID", "1")
	if os.Getppid() == 1 {
		t.Skip("the test process is a child of pid 1")
	}

	g := New()
	g.Listen("http", "127.0.0.1:0", &http.Server{})

	if err := g.Run(cancelSoon(t)); err != nil {
		t.Fatalf("Run should shut down cleanly, got %v", err)
	}

	want := []string{"READY=1", "STOPPING=1"}
	for _, state := range want {
		if got := readState(t, manager); got != state {
			t.Fatalf("an unadopted process must not claim the main role; service manager got %q, want %q", got, state)
		}
	}
}

func TestRun_StaysSilentWithoutNotifySocket(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "")

	g := New()
	g.Listen("http", "127.0.0.1:0", &http.Server{})

	if err := g.Run(cancelSoon(t)); err != nil {
		t.Fatalf("Run should not care about a missing notify socket, got %v", err)
	}
}

func TestNotifier_ReloadingCarriesMonotonicClock(t *testing.T) {
	manager := notifySocket(t)

	New().notify.reloading()

	got := readState(t, manager)
	if !strings.HasPrefix(got, "RELOADING=1\nMONOTONIC_USEC=") {
		t.Fatalf("reload must open with the monotonic timestamp systemd pairs it with, got %q", got)
	}
	if _, err := strconv.ParseInt(strings.TrimPrefix(got, "RELOADING=1\nMONOTONIC_USEC="), 10, 64); err != nil {
		t.Fatalf("MONOTONIC_USEC should be a decimal integer, got %q", got)
	}
}

// notifySocket fakes systemd's notification socket and points NOTIFY_SOCKET at it.
func notifySocket(t *testing.T) *net.UnixConn {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("unix datagram sockets are not supported on windows")
	}

	// not t.TempDir(): unix socket paths are capped around a hundred bytes
	path := filepath.Join(os.TempDir(), fmt.Sprintf("graceful-%d.sock", time.Now().UnixNano()))
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatalf("listen on %s: %v", path, err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = os.Remove(path)
	})
	t.Setenv("NOTIFY_SOCKET", path)
	return conn
}

func readState(t *testing.T, conn *net.UnixConn) string {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read notification: %v", err)
	}
	return string(buf[:n])
}
