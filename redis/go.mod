module github.com/LouisRiverstone/perago/redis

go 1.25.0

require (
	github.com/LouisRiverstone/perago v0.1.0
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/redis/go-redis/v9 v9.17.3
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
)

// Local development: the core module next door. Consumers resolve the tagged version.
replace github.com/LouisRiverstone/perago => ..
