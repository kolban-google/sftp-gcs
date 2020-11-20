IMAGE_LOCATION=us-central1-docker.pkg.dev/test1-253523/repo1/sftp-gcs

all:
	@echo "run - Run the sft-gcs demon locally"
	@echo "cloudbuild - Run Cloudbuild to build an image and store in the repository"
	@echo "dockerbuild - Run docker to build a local image"

run:
	#node index.js --bucket kolban-test1 --port 9022 --service-account-key-file=keys/sftp-gcs-sa.json
	node index.js --bucket kolban-test1 --port 9022

cloudbuild:
	gcloud builds submit . --tag=$(IMAGE_LOCATION)
	@echo "New image now available at $(IMAGE_LOCATION)"

dockerbuild:
	docker build . --tag sftp-gcs