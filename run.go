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

// Run starts every entry, blocks until ctx is cancelled, a component fails or
// an upgraded process takes over, then drains in reverse order and returns the
// cause joined with any drain failures.
func (g *Group) Run(ctx context.Context) error {
	up, err := newUpgrader(g.opts.upgrade)
	if err != nil {
		return err
	}
	defer up.Stop()

	errCh := make(chan namedErr, len(g.entries))
	started, err := g.start(up, errCh)
	if err == nil {
		// before Ready releases the parent, so a reload completes once this process serves
		g.notify.ready()
		err = up.Ready()
	}
	if err == nil {
		done := make(chan struct{})
		go g.notify.adopt(done)
		err = g.await(ctx, up, errCh)
		close(done)
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

// await blocks until shutdown and returns its cause, nil unless a component
// failed; SIGHUP upgrades instead of returning.
func (g *Group) await(ctx context.Context, up upgrader, errCh <-chan namedErr) error {
	var hup chan os.Signal // nil without upgrades, so its case never fires
	if up.CanUpgrade() {
		hup = make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		defer signal.Stop(hup)
	}

	for {
		select {
		case <-ctx.Done():
			g.opts.log.Info("shutdown requested")
			g.notify.stopping()
			return nil
		case <-hup:
			g.opts.log.Info("upgrade requested")
			g.notify.reloading()
			if err := up.Upgrade(); err != nil {
				g.opts.log.Error("upgrade failed", slog.Any("err", err))
				// closes the reload cycle with this process still in charge
				g.notify.ready()
			}
		case ne := <-errCh:
			g.opts.log.Error("component failed", slog.String("name", ne.name), slog.Any("err", ne.err))
			g.notify.stopping()
			return fmt.Errorf("%s: %w", ne.name, ne.err)
		case <-up.Exit():
			// no STOPPING=1: the service keeps running in the child
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
