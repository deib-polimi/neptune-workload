import argparse
import math
import time

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

nodes_address = [
    "https://www.google.com",
    "https://www.amazon.com",
    "https://www.microsoft.com",
    "https://www.facebook.com",
]

node_coordinates = np.array([
    [0.25, 0.25],
    [0.25, 0.75],
    [0.75, 0.25],
    [0.75, 0.75],
])

class UserTasks(TaskSet):
    mode = "network"
    req_session = 5
    counter = 0

    @task
    def get_root(self):
        if self.mode == "multi":
            pass
        else:
            self.client.post(f"/function/{self.mode}", json=json_data[self.mode])
            self.counter = self.counter + 1
            time.sleep(0.1)
            if (self.counter % self.req_session) == 0:
                self.client.close()


class WebsiteUser(HttpUser):
    wait_time = constant(0.8)
    tasks = [UserTasks]


class CustomShape(LoadTestShape):
    time_limit = 0
    user_factory: UserFactory

    def tick(self):
        current_time = round(self.get_run_time())

        if current_time < self.time_limit:
            users = sum(self.user_factory.get_workload(current_time/self.time_limit))
            return round(users), 1
        else:
            return None


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--mode", default="network", type=str)
    parser.add_argument("--duration", default=1200, type=int)
    parser.add_argument("--req_session", default=5, type=int)
    parser.add_argument("--workload", type=str)
    args_dict = vars(parser.parse_args())
    print(args_dict)
    CustomShape.time_limit = args_dict['duration']

    if args_dict['workload'] == "cabspotting":
        CustomShape.user_factory = CabspottingUserFactory("cabspottingdata", node_coordinates)
    elif args_dict['workload'] == "tdrive":
        CustomShape.user_factory = TDriveUserFactory("tdrive", node_coordinates)
    elif args_dict['workload'] == "telecom":
        CustomShape.user_factory = TelecomUserFactory("telecom", node_coordinates)
    else:
        raise Exception(f"not valid workload {args_dict['workload']}")
    UserTasks.mode = args_dict['mode']
    UserTasks.req_session = args_dict['req_session']
