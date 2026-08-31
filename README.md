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
  functions: `start` may block for the component's whole life (an accept
  loop) or return immediately after spawning its own work (a scheduler). A
  non-nil error from any `start` shuts the whole group down.
- **Listeners built for upgrades.** `Listen(name, addr, srv)` creates the
  listener inside `Run` — through tableflip when upgrades are enabled — so an
  upgraded process inherits the socket without dropping connections.
  `*http.Server` satisfies the `Server` interface directly.
- **One shutdown story.** A cancelled context, a component failure, or an
  upgrade handoff all funnel into the same drain: every started component is
  stopped in reverse registration order, under one shared deadline, and drain
  failures come back joined onto the returned error.
- **Zero-downtime upgrades.** `WithUpgrade()` makes SIGHUP re-exec the binary
  and hand off listeners; it silently falls back to plain listeners on
  Windows.
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

`Run` starts entries in registration order and drains them in reverse: the
HTTP listener above stops accepting before the scheduler is asked to finish,
so in-flight requests can still schedule work.

| Trigger | Behavior |
|---|---|
| ctx cancelled (e.g. SIGINT/SIGTERM via `signal.NotifyContext`) | stop accepting, drain every component, return nil |
| a `start` returns non-nil | drain every component, return `name: err` |
| SIGHUP (with `WithUpgrade`) | re-exec the binary, hand listeners to the child, drain, return nil |

## Design notes

- **`start` errors are fatal, `stop` errors are collected.** A component that
  cannot run means the process is broken — everything comes down. A component
  that cannot stop cleanly must not block the rest from draining, so its
  error is logged and joined onto `Run`'s return value instead.
- **A scheduler-style `start` that returns nil is a successful launch**, not
  a failure — only non-nil errors trigger shutdown. This makes `Add` fit both
  blocking accept loops and fire-and-forget starters without adapters.
- **Registration order is the dependency order.** Register infrastructure
  first, entry points last; reverse-order draining then closes the front door
  before the back office.
- **The caller owns shutdown, the group owns SIGHUP.** Shutdown arrives
  through the context (`signal.NotifyContext` in main), so the group composes
  with any cancellation source and tests need no real signals; SIGHUP stays
  internal because upgrades are the group's own feature.
- **The group owns lifecycles, not resources.** Database pools, log writers
  and the like belong to whatever built them (a DI container's cleanup); the
  group only coordinates starting and stopping.

## License

MIT
