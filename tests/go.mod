module github.com/beyondstorage/go-stream/tests

go 1.16

require (
	github.com/beyondstorage/go-service-memory v0.1.0
	github.com/beyondstorage/go-service-s3/v2 v2.3.0
	github.com/beyondstorage/go-storage/v4 v4.4.0
	github.com/beyondstorage/go-stream v0.1.0
	github.com/stretchr/testify v1.7.0
)

replace github.com/beyondstorage/go-stream => ../
