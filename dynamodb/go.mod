module github.com/LouisRiverstone/perago/dynamodb

go 1.25.0

require (
	github.com/LouisRiverstone/perago v0.1.0
	github.com/aws/aws-sdk-go-v2 v1.48.0
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.74.1
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.13.12 // indirect
	github.com/aws/smithy-go v1.28.9 // indirect
)

// Local development: the core module next door. Consumers resolve the tagged version.
replace github.com/LouisRiverstone/perago => ..
