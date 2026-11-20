//go:build !linux

package perago

// totalMemory is unknown outside Linux; MemoryPercent is ignored there.
func totalMemory() uint64 { return 0 }
