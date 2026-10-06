package graceful

import (
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"
)

// adoptInterval is how often an upgraded process checks whether the service
// manager has become its parent.
const adoptInterval = 100 * time.Millisecond

// notifier speaks the sd_notify protocol to the service manager that started
// the process, the one behind systemd's Type=notify and Type=notify-reload
// units. Without $NOTIFY_SOCKET every call is a no-op.
type notifier struct {
	path    string
	manager int // pid of the service manager; the parent of a supervised main process
	log     *slog.Logger
}

func newNotifier(log *slog.Logger) notifier {
	n := notifier{path: os.Getenv("NOTIFY_SOCKET"), manager: 1, log: log}
	// a user manager is not pid 1 and says so
	if pid, err := strconv.Atoi(os.Getenv("MANAGERPID")); err == nil {
		n.manager = pid
	}
	return n
}

// ready reports the process as serving. It also claims the main role when
// the manager is already its parent; an upgraded child leaves that to adopt.
func (n notifier) ready() {
	if n.adopted() {
		n.send(mainPID() + "\nREADY=1")
		return
	}
	n.send("READY=1")
}

// adopt claims the main role once the manager has become the parent, which
// happens when the process that spawned this one exits after a handoff.
// Claiming it earlier would make systemd supervise a process that is not
// its child: it then cannot wait for it, and a stop goes straight to
// SIGKILL. done ends the wait.
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

// reloading opens a reload cycle; Type=notify-reload requires the monotonic
// timestamp to pair it with the READY=1 that closes it.
func (n notifier) reloading() {
	n.send("RELOADING=1\nMONOTONIC_USEC=" + strconv.FormatInt(monotonicUSec(), 10))
}

func (n notifier) stopping() {
	n.send("STOPPING=1")
}

// adopted reports whether the manager is this process's parent, the only
// relation under which it supervises a main process properly.
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
