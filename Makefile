
APP_NAME=static-consumer
VERSION=latest

.PHONY: help

help: ## This help.
	 @awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

# Python Tasks
# execute tests with coverage
test: ## Executes unit tests with coverage
	pytest app --cov-report xml:build/cov.xml --cov app.static_assignment

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
up: ## Spin up the project
	docker-compose up --build $(APP_NAME) -d
