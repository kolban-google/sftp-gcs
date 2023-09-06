## Deploying SFTP Server
### Place All the Kubernetes Deployment Files for creating SFTP Server here in this folder

1) Use `deployments/sftp-test-deployment.yaml` as reference and change `sftp-test` with `sftp-<SCHOOL_NAME>` in all the places including file name.

2) Un-comment  `loadBalancerIP` and provide the IP.

3) Change the Google Cloud Storage bucket name.

4) Create secrets in Kuberbetes for the School using UI or `kubectl create secret generic sftp-insead-credentials --from-literal=user=em_<SCHOOL_NAME> --from-literal=password=<PASSWORD>`.

    ###### Note: Make sure kubectl is set to `emeritus-ds-1` cluster (If not use `kubectl config use-context gke_emeritus-data-science_us-central1-c_emeritus-ds-1`).

5) Push the changes and create a PR to  deploy the server.

## Accessing SFTP Server

1) Install SFTP client in local machine

2) Use the command ```sftp -P 9022 "em_<SCHOOL_NAME>@<IP>"```

3) Provide `yes` when it asks for `Are you sure you want to continue connecting`

4) Provide `<PASSWORD>`
