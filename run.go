package graceful

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

type namedErr struct {
	name string
	err  error
}

// Run starts every entry in registration order, then blocks until ctx is
// cancelled, a component fails, or an upgraded process takes over. It drains
// the started entries in reverse order and returns the failure that caused
// the shutdown joined with any drain failures; a requested shutdown that
// drains cleanly returns nil. Call it once.
func (g *Group) Run(ctx context.Context) error {
	up, err := newUpgrader(g.opts.upgrade)
	if err != nil {
		return err
	}
	defer up.Stop()

	errCh := make(chan namedErr, len(g.entries))
	started, err := g.start(up, errCh)
	if err == nil {
		err = up.Ready()
	}
	if err == nil {
		err = g.await(ctx, up, errCh)
	}
	return errors.Join(err, g.drain(started))
}

// start launches the entries and returns the ones that must be drained; a
// listener that cannot bind aborts the remainder.
func (g *Group) start(up upgrader, errCh chan<- namedErr) ([]entry, error) {
	var started []entry
	for _, e := range g.entries {
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
			return started, fmt.Errorf("%s: listen %s: %w", e.name, e.addr, err)
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
	return started, nil
}

// await blocks until a shutdown trigger and returns its cause: nil for a
// cancelled context or an upgrade handoff, the component's error otherwise.
// SIGHUP triggers an upgrade instead of stopping the group.
func (g *Group) await(ctx context.Context, up upgrader, errCh <-chan namedErr) error {
	var hup chan os.Signal // stays nil without upgrades; nil never receives
	if up.CanUpgrade() {
		hup = make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		defer signal.Stop(hup)
	}

	for {
		select {
		case <-ctx.Done():
			g.opts.log.Info("shutdown requested")
			return nil
		case <-hup:
			g.opts.log.Info("upgrade requested")
			if err := up.Upgrade(); err != nil {
				g.opts.log.Error("upgrade failed", slog.Any("err", err))
			}
		case ne := <-errCh:
			g.opts.log.Error("component failed", slog.String("name", ne.name), slog.Any("err", ne.err))
			return fmt.Errorf("%s: %w", ne.name, ne.err)
		case <-up.Exit():
			g.opts.log.Info("upgrade handed off, draining")
			return nil
		}
	}
}

// drain stops the started entries in reverse order under one shared deadline
// and returns the joined failures.
func (g *Group) drain(started []entry) error {
	ctx, cancel := context.WithTimeout(context.Background(), g.opts.shutdownTimeout)
	defer cancel()

	var errs []error
	for i := len(started) - 1; i >= 0; i-- {
		e := started[i]
		if err := e.stop(ctx); err != nil {
			g.opts.log.Error("component stop failed", slog.String("name", e.name), slog.Any("err", err))
			errs = append(errs, fmt.Errorf("stop %s: %w", e.name, err))
			continue
		}
		g.opts.log.Info("component stopped", slog.String("name", e.name))
	}
	return errors.Join(errs...)
}
