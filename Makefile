help:
	@echo "Available targets:"
	@echo "- test: run tests"
	@echo "- deps: installs dependencies with glide"

deps:
	glide up

test: deps
	go test -i && go test
