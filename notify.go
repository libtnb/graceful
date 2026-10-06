package graceful

import (
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"
)

// adoptInterval is how often an upgraded process checks whether it has been adopted.
const adoptInterval = 100 * time.Millisecond

// notifier speaks sd_notify to the service manager; without $NOTIFY_SOCKET it is a no-op.
type notifier struct {
	path    string
	manager int
	log     *slog.Logger
}

func newNotifier(log *slog.Logger) notifier {
	n := notifier{path: os.Getenv("NOTIFY_SOCKET"), manager: 1, log: log}
	// a user manager is not pid 1 and exports MANAGERPID
	if pid, err := strconv.Atoi(os.Getenv("MANAGERPID")); err == nil {
		n.manager = pid
	}
	return n
}

// ready reports the process as serving, claiming MAINPID too if already adopted.
func (n notifier) ready() {
	if n.adopted() {
		n.send(mainPID() + "\nREADY=1")
		return
	}
	n.send("READY=1")
}

// adopt claims MAINPID once the old process has exited and the manager has
// become the parent, or gives up when done closes.
func (n notifier) adopt(done <-chan struct{}) {
	if n.path == "" || n.adopted() {
		return
	}
	ticker := time.NewTicker(adoptInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if n.adopted() {
				n.send(mainPID() + "\nREADY=1")
				return
			}
		}
	}
}

// reloading opens a reload cycle; Type=notify-reload requires MONOTONIC_USEC with it.
func (n notifier) reloading() {
	n.send("RELOADING=1\nMONOTONIC_USEC=" + strconv.FormatInt(monotonicUSec(), 10))
}

func (n notifier) stopping() {
	n.send("STOPPING=1")
}

// adopted reports whether the manager is the parent; systemd cannot wait for a
// main process that is not its child and would SIGKILL it on stop.
func (n notifier) adopted() bool {
	return os.Getppid() == n.manager
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

func mainPID() string {
	return "MAINPID=" + strconv.Itoa(os.Getpid())
}
