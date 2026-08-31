// Package graceful orchestrates the lifecycle of long-running components.
// A Group starts every registered component together, waits for a shutdown
// signal (SIGINT/SIGTERM) or the first failure, then drains everything in
// reverse registration order within a bounded timeout. With WithUpgrade a
// SIGHUP performs a zero-downtime binary upgrade via tableflip: listeners
// created through Listen are inherited by the new process.
package graceful

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Server is the accepting half of an HTTP(ish) server: Serve blocks on the
// listener until Shutdown drains it. *http.Server satisfies it directly.
type Server interface {
	Serve(ln net.Listener) error
	Shutdown(ctx context.Context) error
}

type options struct {
	log             *slog.Logger
	shutdownTimeout time.Duration
	upgrade         bool
}

type Option func(*options)

// WithLogger sets the logger for lifecycle events. Default slog.Default().
func WithLogger(log *slog.Logger) Option {
	return func(o *options) { o.log = log }
}

// WithShutdownTimeout bounds the total drain time on shutdown. Default 30s.
func WithShutdownTimeout(d time.Duration) Option {
	return func(o *options) { o.shutdownTimeout = d }
}

// WithUpgrade enables zero-downtime binary upgrades on SIGHUP via tableflip.
// It is a no-op on Windows, where the group falls back to plain listeners.
func WithUpgrade() Option {
	return func(o *options) { o.upgrade = true }
}

// entry is one registered component, kept in registration order.
type entry struct {
	name  string
	start func() error                    // nil for listeners; run in a goroutine, may block
	stop  func(ctx context.Context) error // drained in reverse order
	addr  string                          // non-empty marks a listener entry
	srv   Server
}

// Group runs components together. Register with Add and Listen, then call
// Run once; Group is not safe for concurrent registration.
type Group struct {
	opts    options
	entries []entry
}

func New(opts ...Option) *Group {
	o := options{
		log:             slog.Default(),
		shutdownTimeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &Group{opts: o}
}

// Add registers a component. start runs in a goroutine and may block for the
// component's whole life (an accept loop) or return nil immediately after
// spawning its own work (a scheduler): returning a non-nil error shuts the
// whole group down. stop is called during shutdown, bounded by the group's
// shutdown timeout, in reverse registration order.
func (g *Group) Add(name string, start func() error, stop func(ctx context.Context) error) {
	g.entries = append(g.entries, entry{name: name, start: start, stop: stop})
}

// Listen registers srv to serve on a TCP listener bound to addr. The
// listener is created by Run — through tableflip under WithUpgrade, so
// upgraded processes inherit it. http.ErrServerClosed is treated as a clean
// exit.
func (g *Group) Listen(name, addr string, srv Server) {
	g.entries = append(g.entries, entry{name: name, addr: addr, srv: srv})
}

type namedErr struct {
	name string
	err  error
}

// Run starts every entry in registration order, then blocks until a
// shutdown signal arrives, a component fails, or an upgrade hands off. It
// drains all started entries in reverse order and returns the failure that
// caused the shutdown, if any. Call it once.
func (g *Group) Run() error {
	up, err := newUpgrader(g.opts.upgrade)
	if err != nil {
		return err
	}
	defer up.Stop()

	sigs := []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	if up.CanUpgrade() {
		sigs = append(sigs, syscall.SIGHUP)
	}
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, sigs...)
	defer signal.Stop(sig)

	errCh := make(chan namedErr, len(g.entries))
	var started []entry

	for _, e := range g.entries {
		e := e
		if e.addr == "" {
			started = append(started, e)
			go func() {
				if err := e.start(); err != nil {
					errCh <- namedErr{e.name, err}
				}
			}()
			g.opts.log.Info("component started", slog.String("name", e.name))
			continue
		}

		ln, err := up.Listen("tcp", e.addr)
		if err != nil {
			g.drain(started)
			return fmt.Errorf("%s: listen %s: %w", e.name, e.addr, err)
		}
		e.stop = e.srv.Shutdown
		started = append(started, e)
		go func() {
			if err := e.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- namedErr{e.name, err}
			}
		}()
		g.opts.log.Info("listening", slog.String("name", e.name), slog.String("addr", e.addr))
	}

	if err := up.Ready(); err != nil {
		g.drain(started)
		return err
	}

	var cause error
loop:
	for {
		select {
		case s := <-sig:
			if s == syscall.SIGHUP {
				g.opts.log.Info("upgrade requested")
				if err := up.Upgrade(); err != nil {
					g.opts.log.Error("upgrade failed", slog.Any("err", err))
				}
				continue
			}
			g.opts.log.Info("shutdown signal received", slog.String("signal", s.String()))
			break loop
		case ne := <-errCh:
			cause = fmt.Errorf("%s: %w", ne.name, ne.err)
			g.opts.log.Error("component failed", slog.String("name", ne.name), slog.Any("err", ne.err))
			break loop
		case <-up.Exit():
			g.opts.log.Info("upgrade handed off, draining")
			break loop
		}
	}

	g.drain(started)
	return cause
}

// drain stops every started entry in reverse order, sharing one deadline.
func (g *Group) drain(started []entry) {
	ctx, cancel := context.WithTimeout(context.Background(), g.opts.shutdownTimeout)
	defer cancel()

	for i := len(started) - 1; i >= 0; i-- {
		e := started[i]
		if err := e.stop(ctx); err != nil {
			g.opts.log.Error("component stop failed", slog.String("name", e.name), slog.Any("err", err))
			continue
		}
		g.opts.log.Info("component stopped", slog.String("name", e.name))
	}
}
