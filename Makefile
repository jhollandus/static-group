
APP_NAME=static-consumer
VERSION=latest

.PHONY: help

help: ## This help.
	 @awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

# Python Tasks
# execute tests with coverage
test: ## Executes unit tests with coverage
	mkdir -p build
	pipenv run pytest app --cov-report xml:build/cov.xml --cov app.static_assignment --cov-config=.coveragerc

# Install the app
install: ## Installs the module locally in editable mode
	pipenv install -e .


# DOCKER TASKS

.PHONY: build

# Build the container
build: ## Build the release and develoment container.
	docker build -t $(APP_NAME):$(VERSION) .

.PHONY: up
# Build and run the container
up: ## Spin up the project using docker-compose, go to localhost:9021 to see control center
	docker-compose up -d --scale kafka-load=0
	docker-compose -f docker-compose.yml -f docker-compose-static.yml up -d --scale kafka-consumer=8
	docker-compose up -d --scale kafka-load=1
	open http://localhost:9021

.PHONY: performance-standard
performance-standard: ## Execute a performance review using the standard kafka consumer group coordination
	$(MAKE) build
	tests/test-deployment.sh docker-compose-standard.yml standard-group-consumer

.PHONY: performance-static
performance-static: ## Execute a performance review using the static group coordination
	$(MAKE) build
	tests/test-deployment.sh docker-compose-static.yml static-test-consumer
