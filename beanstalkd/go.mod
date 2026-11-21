module github.com/LouisRiverstone/perago/beanstalkd

go 1.25.0

require (
	github.com/LouisRiverstone/perago v0.1.0
	github.com/beanstalkd/go-beanstalk v0.2.0
)

// Local development: the core module next door. Consumers resolve the tagged version.
replace github.com/LouisRiverstone/perago => ..
