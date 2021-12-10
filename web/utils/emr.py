import boto3


def get_client():
    client = boto3.client("emr", region_name="us-west-2")
    return client


def get_emr_id():
    with open("data/emr_data/emr_id.txt", "r") as emr_id_file:
        emr_id = emr_id_file.read()
    return emr_id


def set_emr_id(new_id):
    with open("data/emr_data/emr_id.txt", "w+") as emr_id_file:
        emr_id_file.write(new_id)


def start_emr():
    print("Starting EMR")
    client = get_client()
    response = client.run_job_flow(
        Name="mha-cluster-2x-m4.2xl",
        ReleaseLabel="emr-6.4.0",
        Applications=[
            {'Name': 'Spark'}
        ],
        Instances={
            "MasterInstanceType": "m4.2xlarge",
            "SlaveInstanceType": "m4.2xlarge",
            "InstanceCount": 3,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False
        },
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )
    emr_id = response.get("JobFlowId")
    print(f"EMR started with cluster id: {emr_id}")
    set_emr_id(emr_id)


def stop_emr():
    cur_emr_id = get_emr_id()
    if cur_emr_id:
        print(f"Stopping EMR {cur_emr_id}")
        client = get_client()
        emr_id = get_emr_id()
        response = client.terminate_job_flows(
            JobFlowIds=[emr_id]
        )
        if response.get("ResponseMetadata").get("HTTPStatusCode") == 200:
            print("Stop request successful")
            set_emr_id("")
        else:
            print("There was an issue in terminating the cluster")
    else:
        print("Current EMR id is null, can't stop cluster!")


def get_emr_status():
    client = get_client()
    cluster_id = get_emr_id()
    if not cluster_id:
        return "No cluster found"
    else:
        try:
            cluster_info = client.describe_cluster(ClusterId=cluster_id)
        except:
            return f"Invalid cluster id {cluster_id}"
        else:
            return cluster_info.get("Cluster").get("Status").get("State")


def run_step_on_cluster(question):
    client = get_client()
    if question == "q1":
        response = client.add_job_flow_steps(
            JobFlowId=get_emr_id(),
            Steps=[
                {
                    'Name': 'Setup Debugging',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['state-pusher-script']
                    }
                },
                {
                    'Name': 'Run Spark',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', 's3://mha-bucket/scripts/mha_gen_weightedSum.py',
                                 's3://mha-bucket/data/13100097.csv', 's3://mha-bucket/q1/',
                                 '0']
                    }
                }
            ]
        )
        return response
    if question == "q2":
        response = client.add_job_flow_steps(
            JobFlowId=get_emr_id(),
            Steps=[
                {
                    'Name': 'Setup Debugging',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['state-pusher-script']
                    }
                },
                {
                    'Name': 'Run Spark',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', 's3://mha-bucket/scripts/mha_gen_weightedSum.py',
                                 's3://mha-bucket/data/13100097.csv', 's3://mha-bucket/q2/',
                                 '1']
                    }
                }
            ]
        )
        return response
