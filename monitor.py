import kubernetes as k
import requests
import pandas as pd
import datetime
import time
import sqlalchemy

k.config.load_incluster_config()
v1 = k.client.CoreV1Api()
w = k.watch.Watch()

engine = sqlalchemy.create_engine('postgresql://user:password@metrics-database.kube-system.svc.cluster.local:5432')


def get_dispatcher_addresses():
    addresses = [pod.status.pod_ip for pod in v1.list_namespaced_pod(namespace="default").items if "dispatcher" in pod.metadata.name]
    return addresses


def get_metrics(pod: k.client.V1Pod) -> dict:
    address = pod.status.pod_ip
    try:
        url = f"http://{address}:8000/metrics/"
        return requests.get(url=url).json()
    except:
        return {
            "response_time": 0.0,
            "response_count": 0.0,
            "throughput": 0.0
        }


while True:
    timestamp = pd.Timestamp.now()
    pod_df = pd.DataFrame(
        columns=[
            "timestamp",
            "container_name",
            "pod_name",
            "pod_address",
            "cpu",
            "mem",
            "response_time"
        ],
    )
    for pod in v1.list_namespaced_pod(namespace="openfaas-fn").items:
        # pod: k.client.V1Pod = result["object"]
        pod_spec: k.client.V1PodSpec = pod.spec
        pod_status: k.client.V1PodStatus = pod.status
        pod_metadata: k.client.V1ObjectMeta = pod.metadata
        pod_name = pod_metadata.name
        pod_address = pod_status.pod_ip
        if pod_status.phase == "Running":
            status = [condition.status for condition in pod_status.conditions if condition.type == "Ready" and
                      datetime.datetime.now().timestamp() - condition.last_transition_time.timestamp() > 15]
            if len(status) == 1 and status[0] == "True":
                for container in pod_spec.containers:
                    container_name = container.name
                    if "http-metrics" not in container_name:
                        container_resources: k.client.V1ResourceRequirements = container.resources
                        container_resources_cpu = container_resources.requests["cpu"]
                        container_resources_mem = container_resources.requests["memory"]
                        metrics = get_metrics(pod)
                        entry = pd.DataFrame({
                            "timestamp": [timestamp],
                            "pod_name": [pod_name],
                            "container_name": [container_name],
                            "pod_address": [str(pod_address)],
                            "cpu": [float(k.utils.parse_quantity(container_resources_cpu))],
                            "mem": [float(k.utils.parse_quantity(container_resources_mem))],
                            'node': [pod_spec.node_name],
                            'response_time': [metrics['response_time']]
                        })
                        pod_df = pd.concat([pod_df, entry])

    pod_df.set_index('timestamp').to_sql("pod_log", engine, if_exists="append")

    pod_scale_df = pd.DataFrame(
        columns=[
            "timestamp",
            "pod_name",
            "cpu_actual",
            "cpu_capped",
        ],
    )
    for pod_scale in k.client.CustomObjectsApi().list_namespaced_custom_object(group="systemautoscaler.polimi.it",
                                                                               version="v1beta1",
                                                                               namespace="openfaas-fn",
                                                                               plural="podscales")['items']:
        pod_scale_metadata = pod_scale['metadata']
        pod_scale_status = pod_scale['status']
        pod_scale_name = pod_scale_metadata['name']
        pod_scale_actual = pod_scale_status['actual']
        pod_scale_capped = pod_scale_status['capped']
        cpu_actual = pod_scale_actual['cpu']
        cpu_capped = pod_scale_actual['cpu']
        entry = pd.DataFrame({
            "timestamp": [timestamp],
            "pod_name": [pod_scale_name],
            "cpu_actual": [float(k.utils.parse_quantity(cpu_actual))],
            "cpu_capped": [float(k.utils.parse_quantity(cpu_capped))],
        })
        pod_scale_df = pd.concat([pod_scale_df, entry])

    pod_scale_df.set_index('timestamp').to_sql("podscale_log", engine, if_exists="append")

    time.sleep(2)
