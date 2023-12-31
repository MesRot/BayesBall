build-and-deploy-dev:
	sam build && sam deploy --config-env=dev

build-and-deploy-prod:
	sam build && sam deploy --config-env=prod
