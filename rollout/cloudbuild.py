import logging
from google.cloud.devtools import cloudbuild_v1
import subprocess, os, datetime


LOG = logging.getLogger(__name__)
LOG.setLevel(level=logging.INFO)
logFormatter = logging.Formatter(
    "%(asctime)s [%(name)s] [%(levelname)-5.5s] %(message)s"
)
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
LOG.addHandler(consoleHandler)


def get_build_history(
    gcp_project_id, cloudbuild_trigger_name, build_status, git_branch_name
):
    # Create a client
    client = cloudbuild_v1.CloudBuildClient()

    # Initialize request argument(s)
    request = cloudbuild_v1.ListBuildsRequest(
        project_id=gcp_project_id,
        filter="substitutions.TRIGGER_NAME={trigger_name} AND status={build_status} AND substitutions.BRANCH_NAME={branch_name} ".format(
            trigger_name=cloudbuild_trigger_name,
            build_status=build_status,
            branch_name=git_branch_name,
        ),
    )
    # Make the request
    page_result = client.list_builds(request=request)
    trigger_ar = []
    for response in page_result:
        if response:
            trigger_ar.append(response.start_time)
    return trigger_ar


def fetch_most_recent_build(prev_builds_date_list, current_date):
    start_time_str = current_date
    if len(prev_builds_date_list) > 0:
        start_time = max(prev_builds_date_list)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    LOG.info(
        "Initiating build for deployment files updated since last build on {}".format(
            start_time_str
        )
    )
    return start_time_str


def perform_gke_cluster_auth(gke_region, cluster_name):
    LOG.info(
        subprocess.run(
            [
                """gcloud container clusters get-credentials {} --region {} \
            --project {}""".format(
                    cluster_name, gke_region, project_id
                )
            ],
            capture_output=True,
            shell=True,
            check=True,
        )
    )


def checkout_from_github():
    perform_gke_cluster_auth(gke_cluster.get("zone"), gke_cluster.get("name"))
    user_name = subprocess.run(
        [
            """kubectl get secret github-credentials  --template={{.data.user}} | base64 --decode"""
        ],
        capture_output=True,
        shell=True,
        check=True,
    )

    password = subprocess.run(
        [
            """kubectl get secret github-credentials  --template={{.data.password}} | base64 --decode"""
        ],
        capture_output=True,
        shell=True,
        check=True,
    )

    subprocess.run(
        [
            "git clone https://{}:{}@github.com/emeritus-tech/sftp-gcs.git".format(
                user_name.stdout.decode("utf-8"), password.stdout.decode("utf-8")
            )
        ],
        capture_output=True,
        shell=True,
        check=True,
    )

    return user_name.stdout.decode("utf-8"), password.stdout.decode("utf-8")


def get_changed_files_root_path(changed_file_list, project_root_path):
    changed_deployments_list = []
    deleted_deployments_list = []

    for fl in changed_file_list:
        if fl:
            name = fl.split("/")[-1]
            if project_root_path:
                if project_root_path == "/":
                    dirpath = "/".join(fl.split("/")[0:-1])
                else:
                    dirpath = project_root_path + "/" + "/".join(fl.split("/")[0:-1])

            else:
                dirpath = project_root_path + "/".join(fl.split("/")[0:-1])

            if "deployments/" in fl and ".yaml" in fl:
                if os.path.exists(dirpath + "/" + name):
                    changed_deployments_list.append(dirpath + "/" + name)
                else:
                    LOG.info(
                        "The Path cannot be found. seems like the {} deployment file has been deleted".format(
                            dirpath + "/" + name
                        )
                    )
                    deleted_deployments_list.append(dirpath + "/" + name)

    LOG.info("Changed deployment files list is {}".format(changed_deployments_list))
    LOG.info("Deleted files list is {}".format(deleted_deployments_list))

    return (
        changed_deployments_list,
        deleted_deployments_list,
    )


def find_changed_files(trigger_date):
    os.chdir(project_root)
    snapshot_branch = "snapshot_branch"  # checkout previous version of a branch as branch with name snapshot_branch
    LOG.info(
        subprocess.run(
            [
                "git checkout %s ; git checkout -b %s `git rev-list -n 1 --first-parent --before='%s' %s`"
                % (branch_name, snapshot_branch, trigger_date, branch_name)
            ],
            capture_output=True,
            shell=True,
            check=True,
        )
    )

    changed_file_list = subprocess.run(
        [
            "git diff --name-only --no-renames origin/{} -- deployments".format(
                branch_name
            )
        ],
        capture_output=True,
        shell=True,
        check=True,
    )
    LOG.info(changed_file_list)
    LOG.info(changed_file_list.stdout.decode("utf-8").split("\n"))

    # checkout most recent changes to get updated file
    LOG.info(
        subprocess.run(
            ["git checkout %s" % branch_name],
            capture_output=True,
            shell=True,
            check=True,
        )
    )

    (changed_deployments_list, deleted_deployments_list) = get_changed_files_root_path(
        changed_file_list.stdout.decode("utf-8").split("\n"), project_root
    )

    return (changed_deployments_list, deleted_deployments_list)


def deploy_kubernetes_yaml_files(updated_yamls):
    perform_gke_cluster_auth(gke_cluster.get("zone"), gke_cluster.get("name"))
    # Naming scheme of GKE clusters context is gke_PROJECT-ID_ZONE_CLUSTER-NAME
    gke_context = "_".join(
        ["gke", project_id, gke_cluster.get("zone"), gke_cluster.get("name")]
    )
    for yaml in updated_yamls:
        try:
            if "sftp-test-deployment.yaml" in yaml:
                LOG.info("Skipping sftp-test-deployment.yaml...")
                continue
            resp_deploy = subprocess.run(
                [
                    """kubectl apply -f {} --context {} """.format(
                        yaml,
                        gke_context,
                    )
                ],
                capture_output=True,
                shell=True,
                check=True,
            )
            LOG.info(resp_deploy)
        except Exception as e:
            LOG.exception(f"Error while applying yaml: {yaml}", stack_info=True)


if __name__ == "__main__":
    try:
        # variables independent of environment
        project_id = "emeritus-data-science"
        project_root = "/var/app/sftp-gcs"

        branch_name = "main"

        gke_cluster = {"name": "emeritus-ds-1", "zone": "us-central1-c"}

        cloud_build_trigger_name = "sftp-gcs-ci"

        build_list = get_build_history(
            project_id, cloud_build_trigger_name, "SUCCESS", branch_name
        )
        today_date = str(datetime.datetime.utcnow().date()) + " 00:00:00"
        last_build_date = fetch_most_recent_build(build_list, today_date)
        # last_build_date = "2023-09-01 13:15:00"  # use utc time

        checkout_from_github()

        (changed_deployments_list, deleted_deployments_list) = find_changed_files(
            last_build_date
        )

        if len(changed_deployments_list) > 0:
            deploy_kubernetes_yaml_files(changed_deployments_list)

    except Exception as e:
        LOG.exception("Error in build script", stack_info=True)
        raise e
