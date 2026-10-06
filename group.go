// Package graceful runs long-running components as one group: started
// together, drained in reverse order on shutdown, upgraded in place on
// SIGHUP when WithUpgrade is enabled.
package graceful

import (
	"context"
	"log/slog"
	"net"
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

// Option configures a Group.
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
	start func() error                    // nil for listeners; run in a goroutine
	stop  func(ctx context.Context) error // drained in reverse order
	addr  string                          // non-empty marks a listener entry
	srv   Server
}

// Group runs components together. Register with Add and Listen, then call
// Run once; Group is not safe for concurrent registration.
type Group struct {
	opts    options
	entries []entry
	notify  notifier
}

func New(opts ...Option) *Group {
	o := options{
		log:             slog.Default(),
		shutdownTimeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &Group{opts: o, notify: newNotifier(o.log)}
}

// Add registers a component. start runs in a goroutine: it may block for the
// component's whole life (an accept loop) or return nil after spawning its
// own work (a scheduler); a non-nil error shuts the whole group down. stop
// runs during shutdown in reverse registration order, sharing the group's
// shutdown timeout.
func (g *Group) Add(name string, start func() error, stop func(ctx context.Context) error) {
	g.entries = append(g.entries, entry{name: name, start: start, stop: stop})
}

// Listen registers srv to serve on a TCP listener bound to addr. The
// listener is created by Run — through tableflip under WithUpgrade, so
// upgraded processes inherit it. http.ErrServerClosed is a clean exit.
func (g *Group) Listen(name, addr string, srv Server) {
	g.entries = append(g.entries, entry{name: name, addr: addr, srv: srv})
}
