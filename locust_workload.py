import numpy as np
from locust import TaskSet, task, HttpUser, constant, LoadTestShape, events
from dataset import CabspottingUserFactory, TelecomUserFactory, TDriveUserFactory, UserFactory

json_data = {
    'network': {'url': 'https://sample-videos.com/img/Sample-jpg-image-50kb.jpg'},
    'sleep': {'sleep': 1},
    'dynamic_html': {'username': 'dragonbanana', 'random_len': 80000},
    'thumbnailer': {'url': 'https://sample-videos.com/img/Sample-jpg-image-50kb.jpg', 'width': 10, 'height': 10,
                    'n': 30000},
    'video_processing': {
        'url': 'https://freetestdata.com/wp-content/uploads/2022/02/Free_Test_Data_1MB_MP4.mp4',
        'duration': 2},
    'compression': {'url': 'https://cdn.bestmovie.it/wp-content/uploads/2020/05/winnie-the-pooh-disney-plus-HP.jpg',
                    'compression_mode': 1000},
    # zip, gztar, xztar, bztar
    'image_recognition': {'url': 'https://sample-videos.com/img/Sample-jpg-image-50kb.jpg'},
    'pagerank': {'size': 1500},
    'graph_mst': {'size': 25000},
    'graph_bfs': {'size': 30000},
}

import kubernetes as k

k.config.load_incluster_config()
v1 = k.client.CoreV1Api()
w = k.watch.Watch()


def get_dispatcher_addresses():
    addresses = [pod.status.pod_ip for pod in v1.list_namespaced_pod(namespace="default").items if
                 "dispatcher" in pod.metadata.name]
    return addresses


# If using kubernetes
nodes_address = get_dispatcher_addresses()
# Otherwise put addresses
# nodes_adress = ["https://www.google.com"]

node_coordinates = np.array([
    [0.25, 0.25],
    [0.25, 0.75],
    [0.75, 0.25],
    [0.75, 0.75],
])

functions = [
    "compression",
    "dynamic_html",
    "graph_bfs",
    "graph_mst",
    "thumbnailer",
    "video_processing"
]
functions_weights = np.array([
    5, 5, 1, 5, 5, 0.02
])
functions_weights = functions_weights / sum(functions_weights)

workload_distribution = np.ones(shape=(len(nodes_address), len(functions_weights)))
workload_distribution = workload_distribution / sum(workload_distribution)


class UserTasks(TaskSet):

    @task
    def request(self):
        global workload_distribution
        node_id = np.random.choice(range(len(nodes_address)), p=workload_distribution.sum(axis=1))
        function_id = np.random.choice(range(len(functions)),
                                       p=workload_distribution[node_id] / workload_distribution[node_id].sum())
        node = nodes_address[node_id]
        function = functions[function_id]
        self.client.post(f"http://{node}:8080/function/openfaas-fn/{function.replace('_', '-')}", json=json_data[function])


class WebsiteUser(HttpUser):
    wait_time = constant(1)
    tasks = [UserTasks]


class CustomShape(LoadTestShape):
    time_limit = 0
    user_factory: UserFactory

    def tick(self):
        global workload_distribution
        current_time = round(self.get_run_time())
        if current_time < self.time_limit:
            users = self.user_factory.get_user_workload(current_time / self.time_limit)
            n_users = users.sum()
            workload_distribution = users / n_users
            return round(n_users/20), 1
        else:
            return None


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--duration", default=300, type=int)
    parser.add_argument("--workload", type=str)
    args_dict = vars(parser.parse_args())
    print(args_dict)
    CustomShape.time_limit = args_dict['duration']

    if args_dict['workload'] == "cabspotting":
        CustomShape.user_factory = CabspottingUserFactory("cabspottingdata", node_coordinates, functions_weights)
    elif args_dict['workload'] == "tdrive":
        CustomShape.user_factory = TDriveUserFactory("tdrive", node_coordinates, functions_weights)
    elif args_dict['workload'] == "telecom":
        CustomShape.user_factory = TelecomUserFactory("telecom", node_coordinates, functions_weights)
    else:
        raise Exception(f"not valid workload {args_dict['workload']}")
    UserTasks.mode = args_dict['mode']
    UserTasks.req_session = args_dict['req_session']


@events.request.add_listener
def my_request_handler(request_type, name, response_time, response_length, response,
                       context, exception, start_time, url, **kwargs):
    if exception:
        print(f"Request to {name} with url {url} failed with exception {exception}")
