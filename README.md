# graceful

[![Doc](https://pkg.go.dev/badge/github.com/libtnb/graceful)](https://pkg.go.dev/github.com/libtnb/graceful)
[![Go](https://img.shields.io/github/go-mod/go-version/libtnb/graceful)](https://go.dev/)
[![Release](https://img.shields.io/github/release/libtnb/graceful.svg)](https://github.com/libtnb/graceful/releases)
[![Test](https://github.com/libtnb/graceful/actions/workflows/test.yml/badge.svg)](https://github.com/libtnb/graceful/actions)
[![Report Card](https://goreportcard.com/badge/github.com/libtnb/graceful)](https://goreportcard.com/report/github.com/libtnb/graceful)
[![Stars](https://img.shields.io/github/stars/libtnb/graceful?style=flat)](https://github.com/libtnb/graceful)
[![License](https://img.shields.io/github/license/libtnb/graceful)](https://opensource.org/license/MIT)

Graceful shutdown for long-running Go components: one group, reverse-order
draining, and optional zero-downtime upgrades on SIGHUP via
[tableflip](https://github.com/cloudflare/tableflip).

## Features

- **Any component, not just HTTP.** `Add(name, start, stop)` takes a pair of
  functions. `start` may block for the component's life or return after
  spawning its own work; a non-nil error shuts the whole group down.
- **Listeners built for upgrades.** `Listen(name, addr, srv)` creates the
  listener inside `Run`, through tableflip when upgrades are enabled, so an
  upgraded process inherits the socket. `*http.Server` satisfies `Server`.
- **One shutdown path.** Cancellation, component failure and upgrade handoff
  all drain the same way: reverse registration order, one shared deadline,
  drain failures joined onto the returned error.
- **Zero-downtime upgrades.** `WithUpgrade()` makes SIGHUP re-exec the binary
  and hand off listeners; Windows falls back to plain listeners.
- **systemd aware.** Under `NOTIFY_SOCKET` the group reports its own state, so
  a `Type=notify-reload` unit gets a synchronous, zero-downtime
  `systemctl reload`.
- **Structured logging** of every lifecycle event through `log/slog`.

## Install

```bash
go get github.com/libtnb/graceful
```

Requires Go 1.27+.

## Quick start

```go
package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/libtnb/graceful"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Handler: mux}

	g := graceful.New(
		graceful.WithUpgrade(),                      // SIGHUP = hot upgrade
		graceful.WithShutdownTimeout(30*time.Second),
	)
	g.Add("cron", cron.Start, cron.Stop)             // any start/stop pair
	g.Listen("http", ":8080", srv)                   // upgrade-aware listener

	if err := g.Run(ctx); err != nil {               // blocks until shutdown
		log.Fatal(err)
	}
}
```

`Run` starts entries in registration order and drains them in reverse, so the
listener above stops accepting before the scheduler stops and in-flight
requests can still schedule work.

| Trigger | Behavior |
|---|---|
| ctx cancelled (e.g. SIGINT/SIGTERM via `signal.NotifyContext`) | stop accepting, drain every component, return nil (a `*DrainError` if a component failed to stop) |
| a `start` returns non-nil | drain every component, return `name: err` |
| SIGHUP (with `WithUpgrade`) | re-exec the binary, hand listeners to the child, drain, return nil |

## systemd

Nothing to configure in Go. With `NOTIFY_SOCKET` set, the group sends
`READY=1` once every listener accepts, brackets a SIGHUP upgrade with
`RELOADING=1` and the child's `READY=1` + `MAINPID=`, and sends `STOPPING=1`
on shutdown. That is the whole `Type=notify-reload` contract, so
`systemctl reload` becomes a zero-downtime upgrade that returns once the new
process serves:

```ini
[Service]
Type=notify-reload
NotifyAccess=all
ExitType=cgroup
ExecStart=/opt/app/app
```

`NotifyAccess=all` lets the upgraded process report before systemd knows it
as the main one. `ExitType=cgroup` keeps the unit up while the old process
drains and exits, because the child claims `MAINPID=` only after systemd
adopts it: systemd cannot wait for a main process that is not its child, so a
stop would escalate straight to SIGKILL. Without `WithUpgrade()` the reload's
SIGHUP terminates the process. Replace the binary in place before reloading;
the child re-executes the same path.

## Design notes

- **`start` errors are fatal, `stop` errors are collected.** A component that
  cannot run brings everything down. One that cannot stop must not block the
  rest, so its error is logged and joined onto `Run`'s result.
- **A `start` that returns nil is a successful launch**, so `Add` fits both
  blocking accept loops and fire-and-forget starters without adapters.
- **Registration order is the dependency order.** Register infrastructure
  first and entry points last; reverse draining closes the front door before
  the back office.
- **The caller owns shutdown, the group owns SIGHUP.** Shutdown arrives through
  the context, so any cancellation source works and tests need no real
  signals; SIGHUP stays internal because upgrades are the group's own feature.
- **The group owns lifecycles, not resources.** Pools, log writers and the like
  belong to whatever built them; the group only coordinates start and stop.

## License

MIT
