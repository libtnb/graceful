package graceful

import "golang.org/x/sys/unix"

// monotonicUSec is CLOCK_MONOTONIC in microseconds, the clock systemd
// expects in MONOTONIC_USEC.
func monotonicUSec() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		return 0
	}
	return ts.Nano() / 1000
}
