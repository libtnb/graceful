package graceful

import (
	"log/slog"
	"net"
	"os"
	"strconv"
)

// notifier speaks the sd_notify protocol to the service manager that started
// the process, the one behind systemd's Type=notify and Type=notify-reload
// units. Without $NOTIFY_SOCKET every call is a no-op.
type notifier struct {
	path string
	log  *slog.Logger
}

func newNotifier(log *slog.Logger) notifier {
	return notifier{path: os.Getenv("NOTIFY_SOCKET"), log: log}
}

// ready reports this process as the main one and serving. An upgraded child
// sends it too, so the manager follows the handoff instead of treating the
// parent's exit as the service dying.
func (n notifier) ready() {
	n.send("MAINPID=" + strconv.Itoa(os.Getpid()) + "\nREADY=1")
}

// reloading opens a reload cycle; Type=notify-reload requires the monotonic
// timestamp to pair it with the READY=1 that closes it.
func (n notifier) reloading() {
	n.send("RELOADING=1\nMONOTONIC_USEC=" + strconv.FormatInt(monotonicUSec(), 10))
}

func (n notifier) stopping() {
	n.send("STOPPING=1")
}

func (n notifier) send(state string) {
	if n.path == "" {
		return
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: n.path, Net: "unixgram"})
	if err == nil {
		_, err = conn.Write([]byte(state))
		_ = conn.Close()
	}
	if err != nil {
		n.log.Warn("service manager notification failed", slog.String("state", state), slog.Any("err", err))
	}
}
