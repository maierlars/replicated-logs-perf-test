all: docker-image

docker-image: replicated-logs-perf-test
	docker build . -t maierlars/replicated-logs-perf-test
.PHONY: docker-image

replicated-logs-perf-test: main.go go.sum go.mod
	CGO_ENABLED=0 go build -a -tags netgo -ldflags '-w' .