go build -race -buildmode=plugin ../mrapps/wc.go
go build -race mrcoordinator.go
go build -race mrworker.go
