package engine

import (
	"time"
)

func (e *walEngine) syncLoop() {
	defer close(e.periodicDone)
	ticker := time.NewTicker(e.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.mu.Lock()
			e.waitDurable(e.written)
			e.mu.Unlock()
		case <-e.periodicStop:
			return
		}
	}
}
