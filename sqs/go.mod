module github.com/LouisRiverstone/perago/sqs

go 1.25.0

require (
	github.com/LouisRiverstone/perago v0.1.0
	github.com/aws/aws-sdk-go-v2 v1.48.0
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.21
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.17 // indirect
	github.com/aws/smithy-go v1.28.9 // indirect
)

// Local development: the core module next door. Consumers resolve the tagged version.
replace github.com/LouisRiverstone/perago => ..
