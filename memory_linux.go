//go:build linux

package perago

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// totalMemory is the container limit (cgroup v2 or v1) when set, else MemTotal.
func totalMemory() uint64 {
	for _, p := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
		if err == nil && n > 0 && n < 1<<60 {
			return n
		}
	}
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kb, _ := strconv.ParseUint(fields[1], 10, 64)
			return kb * 1024
		}
	}
	return 0
}
