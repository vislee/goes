help:
	@echo "Available targets:"
	@echo "- test: run tests"
	@echo "- deps: installs dependencies with glide"
	@echo "- watch: watch for changes and re-run tests"

deps:
	glide install

test: deps
	ginkgo -race -randomizeAllSpecs -r -skipPackage vendor -progress .

watch: deps
	ginkgo watch -race -randomizeAllSpecs -r -skipPackage vendor -progress -notify .
