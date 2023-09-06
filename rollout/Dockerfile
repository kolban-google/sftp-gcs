FROM python:3.7.7-stretch
RUN apt-get update
RUN apt-get install -y libsnappy-dev
RUN apt-get install -y curl
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y
RUN apt-get install -y google-cloud-sdk-gke-gcloud-auth-plugin
RUN apt-get install -y kubectl

RUN mkdir /var/app
ADD rollout/cloudbuild.py /var/app

WORKDIR /var/app

ENV PYTHONPATH /var/app

RUN pip3 install --upgrade --no-cache-dir -r rollout/requirements.txt

ENTRYPOINT ["python3"]
CMD ["--version"]
