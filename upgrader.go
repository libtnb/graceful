package graceful

import (
	"net"
	"runtime"

	"github.com/cloudflare/tableflip"
)

// upgrader abstracts listener creation and the handoff to an upgraded binary.
type upgrader interface {
	Listen(network, addr string) (net.Listener, error)
	Ready() error
	// Exit closes when an upgraded process takes over; nil means never.
	Exit() <-chan struct{}
	Upgrade() error
	CanUpgrade() bool
	Stop()
}

func newUpgrader(upgrade bool) (upgrader, error) {
	if !upgrade || runtime.GOOS == "windows" {
		return plainUpgrader{}, nil
	}
	upg, err := tableflip.New(tableflip.Options{})
	if err != nil {
		return nil, err
	}
	return flipUpgrader{upg}, nil
}

type plainUpgrader struct{}

func (plainUpgrader) Listen(network, addr string) (net.Listener, error) {
	return net.Listen(network, addr)
}
func (plainUpgrader) Ready() error          { return nil }
func (plainUpgrader) Exit() <-chan struct{} { return nil }
func (plainUpgrader) Upgrade() error        { return nil }
func (plainUpgrader) CanUpgrade() bool      { return false }
func (plainUpgrader) Stop()                 {}

type flipUpgrader struct {
	upg *tableflip.Upgrader
}

func (f flipUpgrader) Listen(network, addr string) (net.Listener, error) {
	return f.upg.Listen(network, addr)
}
func (f flipUpgrader) Ready() error          { return f.upg.Ready() }
func (f flipUpgrader) Exit() <-chan struct{} { return f.upg.Exit() }
func (f flipUpgrader) Upgrade() error        { return f.upg.Upgrade() }
func (f flipUpgrader) CanUpgrade() bool      { return true }
func (f flipUpgrader) Stop()                 { f.upg.Stop() }
