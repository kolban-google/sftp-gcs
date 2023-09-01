import logging
from google.cloud.devtools import cloudbuild_v1
import subprocess, os, argparse, datetime


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
        "Initiating build for proto files updated since last build on {}".format(
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
    perform_gke_cluster_auth("us-central1-c", "emeritus-ds-usa")
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
            "git clone https://{}:{}@github.com/emeritus-tech/seshat.git".format(
                user_name.stdout.decode("utf-8"), password.stdout.decode("utf-8")
            )
        ],
        capture_output=True,
        shell=True,
        check=True,
    )

    return user_name.stdout.decode("utf-8"), password.stdout.decode("utf-8")


def get_changed_files_root_path(changed_file_list, project_root_path):
    changed_protos_list = []
    changed_dataflow_templates_list = []
    changed_unified_config_file_list = []
    changed_api_server_list = []
    deleted_protos_list = []  # for case where an proto file is deleted from repo

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

            if "protos/" in fl and ".proto" in fl:
                if os.path.exists(dirpath + "/" + name):
                    changed_protos_list.append(dirpath + "/" + name)
                else:
                    LOG.info(
                        "The Path cannot be found. seems like the {} proto file has been deleted".format(
                            dirpath + "/" + name
                        )
                    )
                    deleted_protos_list.append(dirpath + "/" + name)

            elif "dataflow/" in fl:
                if "/df_unified_job_config.json" in fl:
                    changed_unified_config_file_list.append(dirpath + "/" + name)
                else:
                    changed_dataflow_templates_list.append(dirpath + "/" + name)

            elif "api/" in fl:
                changed_api_server_list.append(dirpath + "/" + name)

    LOG.info("Changed proto files list is {}".format(changed_protos_list))
    LOG.info("Deleted files list is {}".format(deleted_protos_list))
    LOG.info(
        "changed dataflow template list is {}".format(changed_dataflow_templates_list)
    )
    LOG.info("changed api server list is {}".format(changed_api_server_list))
    LOG.info(
        "changed unified config file list is {}".format(
            changed_unified_config_file_list
        )
    )

    return (
        changed_protos_list,
        changed_dataflow_templates_list,
        changed_api_server_list,
        deleted_protos_list,
        changed_unified_config_file_list,
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

    prev_unified_config_file_list = []
    if os.path.exists("/var/app/seshat/dataflow/df_unified_job_config.json"):
        # reading prev state of unified config list
        f = open("/var/app/seshat/dataflow/df_unified_job_config.json", "r")
        f_json = json.load(f)
        prev_unified_config_file_list = f_json.get("unified_processing")

    LOG.info(f"previous unified config was {prev_unified_config_file_list}")

    changed_file_list = subprocess.run(
        [
            "git diff --name-only --no-renames origin/{} protos dataflow api".format(
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

    (
        changed_protos_list,
        changed_dataflow_templates_list,
        changed_api_server_list,
        deleted_protos_list,
        changed_unified_config_file_list,
    ) = get_changed_files_root_path(
        changed_file_list.stdout.decode("utf-8").split("\n"), project_root
    )

    # schema validation and breaking changes check
    linted_proto_list = run_buf_lint_and_breaking_changes(
        changed_protos_list, snapshot_branch
    )

    LOG.info(
        "Modified files which did not pass linting checks is {}".format(
            list(set(changed_protos_list) - set(linted_proto_list))
        )
    )

    return (
        linted_proto_list,
        deleted_protos_list,
        changed_dataflow_templates_list,
        changed_api_server_list,
        changed_unified_config_file_list,
        prev_unified_config_file_list,
    )


def run_cloud_build_trigger(trigger_name, branch):
    command = f"gcloud beta builds triggers run {trigger_name} --branch={branch}"
    resp = subprocess.run(
        [command],
        capture_output=True,
        shell=True,
        check=True,
    )
    LOG.info(resp)


def fetch_docker_image_status(st_timestamp, image_name):
    command = (
        "gcloud container images list-tags gcr.io/emeritus-data-science/{} "
        '--filter="tags:latest AND TIMESTAMP.datetime>{}" '.format(
            image_name, st_timestamp
        )
    )

    resp = subprocess.run(
        [command],
        capture_output=True,
        shell=True,
        check=True,
    )
    LOG.info(resp)

    if resp.stdout.decode("utf-8").strip():
        return "image uploaded"
    return None


def get_incremented_tag_value(current_tag):
    incremented_tag = (
        current_tag.split(".")[0] + "." + str(int(current_tag.split(".")[-1]) + 1)
    )
    return incremented_tag


def push_to_github_and_create_tag(
    gencode_lang_ar, git_user_name, git_user_email, git_password, push_tag
):
    dir_to_commit = ""
    for lang in gencode_lang_ar:
        dir_to_commit = dir_to_commit + " gencode/" + lang

    LOG.info(f"Pushing dirs {dir_to_commit} to github")

    command = (
        f"git add {dir_to_commit.strip()}; "
        f'git -c user.email={git_user_email} -c user.name={git_user_name} commit -m "lang gencode file"; '
        f"git push"
    )
    try:
        resp = subprocess.run(
            [command],
            capture_output=True,
            shell=True,
            check=True,
        )
        LOG.info(resp)
    except Exception as e:
        LOG.error("Exception while pushing to github {}".format(e))
        raise e

    if push_tag:
        try:
            get_tags_command = "git describe --abbrev=0"
            resp = subprocess.run([get_tags_command], capture_output=True, shell=True)
            LOG.info(resp)

            incremented_tag = "v1.0"
            if resp.returncode == 0:
                incremented_tag = get_incremented_tag_value(
                    resp.stdout.decode("utf-8").strip("\n")
                )

            push_tag_command = (
                f'git -c user.email={git_user_email} -c user.name={git_user_name} tag -a {incremented_tag} -m "new tag";'
                f"git push https://{git_user_name}:{git_password}@github.com/emeritus-tech/seshat.git origin/{branch_name} {incremented_tag}"
            )

            resp = subprocess.run(
                [push_tag_command],
                capture_output=True,
                shell=True,
                check=True,
            )
            LOG.info(resp)
            return incremented_tag
        except Exception as e:
            exception_list.append(e)
            LOG.error("Exception while pushing tag to github {}".format(e))
            raise e
    return None


def deploy_yaml(context, is_config_cluster, updated_yaml_dir):
    try:
        resp_deploy = subprocess.run(
            ["""kubectl apply -k {} --context {} """.format(updated_yaml_dir, context)],
            capture_output=True,
            shell=True,
            check=True,
        )
        LOG.info(resp_deploy)

        if is_config_cluster:
            resp_deploy = subprocess.run(
                [
                    """kubectl apply -f {} --context {} """.format(
                        updated_yaml_dir + "/multicluster_ingress_and_services.yaml",
                        context,
                    )
                ],
                capture_output=True,
                shell=True,
                check=True,
            )
            LOG.info(resp_deploy)

    except Exception as e:
        exception_list.append(e)
        LOG.error("Error while deploying yaml with error {}".format(e))

    # for api_files in updated_yaml_list:
    #     # Not deploying certificate.yaml through CI as it may exceed rate limit for cert-manager
    #     if "certificate.yaml" in api_files:
    #         continue
    #
    #     elif (
    #         "multicluster_ingress_and_services.yaml" in api_files and is_config_cluster
    #     ):
    #         try:
    #             resp_deploy = subprocess.run(
    #                 [
    #                     """kubectl apply -f {} --context {} """.format(
    #                         api_files, context
    #                     )
    #                 ],
    #                 capture_output=True,
    #                 shell=True,
    #                 check=True,
    #             )
    #             LOG.info(resp_deploy)
    #         except Exception as e:
    #             exception_list.append(e)
    #             LOG.error(
    #                 "Error while deploying yaml {} with error {}".format(api_files, e)
    #             )
    #     else:
    #         if "multicluster_ingress_and_services.yaml" in api_files:
    #             pass
    #         else:
    #             try:
    #                 resp_deploy = subprocess.run(
    #                     [
    #                         """kubectl apply -f {} --context {} """.format(
    #                             api_files, context
    #                         )
    #                     ],
    #                     capture_output=True,
    #                     shell=True,
    #                     check=True,
    #                 )
    #                 LOG.info(resp_deploy)
    #             except Exception as e:
    #                 exception_list.append(e)
    #                 LOG.error(
    #                     "Error while deploying yaml {} with error {}".format(
    #                         api_files, e
    #                     )
    #                 )


def rollout_restart(deployments_ar):
    for dep in deployments_ar:
        try:
            resp_rollout_deploy = subprocess.run(
                ["""kubectl rollout restart deployment/{} -n seshat""".format(dep)],
                capture_output=True,
                shell=True,
                check=True,
            )
            LOG.info(resp_rollout_deploy)
        except Exception as e:
            exception_list.append(e)
            LOG.error("Error while deploying  {} with error {}".format(dep, e))


def fetch_current_image_tag(image_name):
    try:
        command = (
            "gcloud container images list-tags gcr.io/emeritus-data-science/{} "
            '--filter="tags:latest" --format "value(TAGS)" '.format(image_name)
        )

        resp = subprocess.run(
            [command],
            capture_output=True,
            shell=True,
            check=True,
        )
        LOG.info(resp)

        if resp.stdout.decode("utf-8").strip():
            return resp.stdout.decode("utf-8").strip().replace("latest", "").strip(",")
    except Exception as e:
        LOG.error("Error while fetching image {} tag {}".format(image_name, e))

    return None


def update_image_tag(image_name, image_tag):
    if image_tag:
        try:
            command = (
                "gcloud container images add-tag gcr.io/emeritus-data-science/{}:{} "
                " gcr.io/emeritus-data-science/{}:latest".format(
                    image_name, image_tag, image_name
                )
            )
            resp = subprocess.run(
                [command],
                capture_output=True,
                shell=True,
                check=True,
            )
            LOG.info(resp)
        except Exception as e:
            LOG.error("Error while updating tag for image {} {}".format(image_name, e))


def delete_github_tag(github_tag):
    try:
        # TODO
        command = "git push --delete origin {}".format(github_tag)
        resp = subprocess.run(
            [command],
            capture_output=True,
            shell=True,
            check=True,
        )
        LOG.info(resp)
    except Exception as e:
        LOG.error("Error while updated tag {} {}".format(github_tag, e))


def deploy_kubernetes_yaml_files(updated_yaml_dir):
    is_config_cluster_present = False
    for c_name in gke_cluster_name:
        perform_gke_cluster_auth(c_name.get("zone"), c_name.get("name"))
        # Naming scheme of GKE clusters context is gke_PROJECT-ID_ZONE_CLUSTER-NAME
        gke_context = "_".join(
            ["gke", project_id, c_name.get("zone"), c_name.get("name")]
        )
        if updated_yaml_dir:
            deploy_yaml(
                gke_context,
                c_name.get("name") == gke_config_cluster_name,
                updated_yaml_dir,
            )
        if c_name.get("name") == gke_config_cluster_name:
            is_config_cluster_present = True
        deployment_ar = ["envoy-deployment", "seshat-app-server"]
        rollout_restart(deployment_ar)

    if not is_config_cluster_present:
        gke_context = "_".join(
            ["gke", project_id, "us-central1-c", gke_config_cluster_name]
        )
        if updated_yaml_dir:
            resp_deploy = subprocess.run(
                [
                    """kubectl apply -f {} --context {} """.format(
                        updated_yaml_dir + "/multicluster_ingress_and_services.yaml",
                        gke_context,
                    )
                ],
                capture_output=True,
                shell=True,
                check=True,
            )
            LOG.info(resp_deploy)


if __name__ == "__main__":
    try:
        # variables independent of environment
        project_id = "emeritus-data-science"
        user_email = "sean.appleby@emeritus.org"
        service_account_dataflow = (
            "seshat-beam-runner@emeritus-data-science.iam.gserviceaccount.com"
        )
        exception_list = []
        project_root = "/var/app/sftp-gcs"

        # one of the clusters from above is config cluster where multi-cluster ingress and service is configured
        gke_config_cluster_name = "emeritus-ds-usa"

        branch_name = "main"

        gke_cluster_name = [
            {"name": "emeritus-ds", "zone": "us-central1-c"},
        ]

        cloud_build_trigger_name = "sftp-gcs"

        build_list = get_build_history(
            project_id, cloud_build_trigger_name, "SUCCESS", branch_name
        )
        today_date = str(datetime.datetime.utcnow().date()) + " 00:00:00"
        last_build_date = fetch_most_recent_build(build_list, today_date)
        # last_build_date = "2023-03-20 07:00:00"  # use utc time

        user_name, password = checkout_from_github()
        (
            updated_protos,
            deleted_protos,
            updated_dataflow_templates,
            updated_api_server_list,
            updated_unified_config_file_list,
            prev_unified_config_file_list,
        ) = find_changed_files(last_build_date)

        if len(updated_api_server_list) > 0 or len(updated_protos) > 0:
            updated_yaml_files = [
                x
                for x in updated_api_server_list
                if "k8s/{}/".format(args.env) in x or "k8s/base/" in x
            ]
            updated_yaml_dir_path = ""
            if len(updated_yaml_files) > 0:
                # since kustomize is now used to manage deployments, the method will accept just the dir path
                updated_yaml_dir_path = "api/k8s/{}".format(args.env)
                LOG.info(
                    f"updated yaml list to be deployed is {updated_yaml_files} and dir is {updated_yaml_dir_path}"
                )
            deploy_kubernetes_yaml_files(updated_yaml_dir_path)

    except Exception as e:
        LOG.error("Error in build script")
        raise e
