package engine

import (
	"fmt"
	"os"
)

// SyncDir fsyncs a directory so that files created, renamed or removed in it
// survive a power loss. Fsyncing a file only covers its contents, not its
// name in the directory.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	return nil
}
