//go:build !linux

package graceful

// monotonicUSec only matters under systemd, which is Linux-only.
func monotonicUSec() int64 { return 0 }
