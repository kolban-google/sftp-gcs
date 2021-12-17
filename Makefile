IMAGE_LOCATION=us-central1-docker.pkg.dev/test1-253523/repo1/sftp-gcs

all:
	@echo "run - Run the sft-gcs demon locally"
	@echo "cloudbuild - Run Cloudbuild to build an image and store in the repository"
	@echo "dockerbuild - Run docker to build a local image"

run:
	#node sftp-gcs.js --bucket kolban-test1 --port 9022 --service-account-key-file=keys/sftp-gcs-sa.jsonX --debug debug
	#node sftp-gcs.js --bucket kolban-test1 --port 9022 --user=user --public-key-file=/home/kolban/.ssh/id_rsa.pub
	#node sftp-gcs.js --bucket kolban-test1 --port 9022 --public-key-file=/home/kolban/.ssh/id_rsa.pub --debug debug
	node sftp-gcs.js --bucket gs://kolban-test-sftp --port 9022 --debug debug
	#node sftp-gcs.js --bucket gs://kolban-test-sftp --port 9022 --debug debug --user neil --password password > /dev/null 2>&1 &
	#node sftp-gcs.js --bucket gs://kolban-test-sftp --port 9022 --debug debug --user neil --password password
	#node sftp-gcs.js --bucket kolban-test1 --port 22

cloudbuild:
	gcloud builds submit . --tag=$(IMAGE_LOCATION)
	@echo "New image now available at $(IMAGE_LOCATION)"

dockerbuild:
	docker build . --tag sftp-gcs

sftp:
	#sftp -v -v -o Port=9022 -o LogLevel=DEBUG3 user@localhost
	sftp -v -v -o Port=9022 -o LogLevel=DEBUG3 localhost