VENV_PATH='az-ge/bin/activate'
DOCKER_NAME='ds_gitdataops'
DOCKER_TAG='0.0.1'
AZURE_CONTAINER_REGISTRY='dndsregistry.azurecr.io'

format:
	black .

env:
	. .env

build:
	docker build -t $(AZURE_CONTAINER_REGISTRY)/$(DOCKER_NAME):$(DOCKER_TAG) .

push:
	docker push $(AZURE_CONTAINER_REGISTRY)/$(DOCKER_NAME):$(DOCKER_TAG)

pull:
	docker pull $(AZURE_CONTAINER_REGISTRY)/$(DOCKER_NAME):$(DOCKER_TAG)

run:
	docker run -it -p 80:80 --env-file .env $(AZURE_CONTAINER_REGISTRY)/$(DOCKER_NAME):$(DOCKER_TAG)

db-up:
	docker-compose -f docker/docker-compose.yml up -d

db-down:
	docker-compose -f docker/docker-compose.yml down

clean-dumps:
	rm -f dumps/*.sql
	rm -f dumps/globals/*.sql