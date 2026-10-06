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

// Server serves a listener until Shutdown drains it; *http.Server satisfies it.
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

// WithLogger sets the logger for lifecycle events (default slog.Default()).
func WithLogger(log *slog.Logger) Option {
	return func(o *options) { o.log = log }
}

// WithShutdownTimeout bounds the total drain time on shutdown (default 30s).
func WithShutdownTimeout(d time.Duration) Option {
	return func(o *options) { o.shutdownTimeout = d }
}

// WithUpgrade enables zero-downtime binary upgrades on SIGHUP via tableflip; a no-op on Windows.
func WithUpgrade() Option {
	return func(o *options) { o.upgrade = true }
}

type entry struct {
	name  string
	start func() error // nil for listeners
	stop  func(ctx context.Context) error
	addr  string // non-empty marks a listener
	srv   Server
}

// Group runs components together: register with Add and Listen, then call Run once.
type Group struct {
	opts    options
	entries []entry
	notify  notifier
}

// New returns a Group configured by opts.
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

// Add registers a component: start runs in a goroutine and may block or return
// nil after spawning work, a non-nil error shuts the group down, and stop runs
// on shutdown in reverse registration order.
func (g *Group) Add(name string, start func() error, stop func(ctx context.Context) error) {
	g.entries = append(g.entries, entry{name: name, start: start, stop: stop})
}

// Listen registers srv on a TCP listener for addr that Run creates, inherited
// across upgrades under WithUpgrade; http.ErrServerClosed counts as a clean exit.
func (g *Group) Listen(name, addr string, srv Server) {
	g.entries = append(g.entries, entry{name: name, addr: addr, srv: srv})
}
