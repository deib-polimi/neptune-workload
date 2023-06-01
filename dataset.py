import pathlib
import re

import numpy as np
import pandas as pd
from scipy import spatial
from sklearn.preprocessing import QuantileTransformer


class UserFactory:

    def __init__(self, users: pd.DataFrame, node_coordinates: np.array):
        self.users = users
        self.node_coordinates = node_coordinates
        self.kd_tree = spatial.KDTree(node_coordinates)

    def get_position(self, timestamp: float):
        filtered_users = self.users[:self.users['start'].searchsorted(timestamp, side='left')]
        filtered_users = filtered_users[filtered_users['end'] >= timestamp]
        return filtered_users

    def get_workload(self, timestamp: float):
        user_coordinates = self.get_position(timestamp)[['lat', 'long']].to_numpy()
        node_workload = [self.kd_tree.query(user)[1] for user in user_coordinates]
        node_ids, value_counts = np.unique(node_workload, return_counts=True)
        result = [0] * len(self.node_coordinates)
        for node_id, value_count in zip(node_ids, value_counts):
            result[node_id] = value_count
        return result

class CabspottingUserFactory(UserFactory):

    def __init__(self, dataset_dir: str, node_coordinates: np.array):
        self.dataset_dir = pathlib.Path(dataset_dir)
        cabs_df = pd.read_csv(self.dataset_dir.joinpath("_cabs.txt"), header=None)
        cabs_df.columns = ['row']
        cabs = cabs_df['row'].map(lambda x: re.findall('"([^"]*)"', x)[0]).to_list()
        cabs_info = []
        for i, cab_id in enumerate(cabs):
            cab_df = pd.read_csv(self.dataset_dir.joinpath(f"new_{cab_id}.txt"), sep=" ", header=None,
                                 names=['lat', 'long', 'state', 'timestamp'], usecols=['lat', 'long', 'timestamp'])
            cab_df['id'] = i
            cab_df = cab_df.sort_values('timestamp')
            timestamps = cab_df['timestamp'].to_list()
            cab_df = cab_df[1:]
            cab_df['start'] = timestamps[:-1]
            cab_df['end'] = timestamps[1:]
            cab_df = cab_df.drop(columns='timestamp')
            cabs_info.append(cab_df)
        users = pd.concat(cabs_info)
        # Normalize towards uniform distribution
        cols = ['lat', 'long']
        scaler = QuantileTransformer()
        users[cols] = scaler.fit_transform(users[cols])
        min_time = users['start'].min()
        max_time = users['end'].max()
        for col in ['start', 'end']:
            users[col] = (users[col] - min_time) / (max_time - min_time)
        users = users.sort_values(['start', 'end'])
        super().__init__(users, node_coordinates)


class TDriveUserFactory(UserFactory):

    def __init__(self, dataset_dir: str, node_coordinates: np.array):
        self.dataset_dir = pathlib.Path(dataset_dir)
        cabs_info = []
        for i in range(1, 10357):
            file = self.dataset_dir.joinpath(f"release/taxi_log_2008_by_id/{i}.txt")
            if file.stat().st_size == 0:
                continue
            cab_df = pd.read_csv(file, sep=",", header=None, index_col=0, parse_dates=['timestamp'],
                                 date_format="%Y-%m-%d %H:%M:%S", names=['timestamp', 'lat', 'long'])
            cab_df.columns = ['timestamp', 'lat', 'long']
            cab_df['id'] = i
            # cab_df['timestamp'] = cab_df['timestamp']
            cab_df = cab_df.sort_values('timestamp')
            timestamps = cab_df['timestamp'].to_list()
            cab_df = cab_df[1:]
            cab_df['start'] = timestamps[:-1]
            cab_df['end'] = timestamps[1:]
            cab_df = cab_df.drop(columns='timestamp')
            cabs_info.append(cab_df)
        users = pd.concat(cabs_info)
        # Normalize towards uniform distribution
        cols = ['lat', 'long']
        scaler = QuantileTransformer()
        users[cols] = scaler.fit_transform(users[cols])
        min_time = users['start'].min()
        max_time = users['end'].max()
        for col in ['start', 'end']:
            users[col] = (users[col] - min_time) / (max_time - min_time)
        users = users.sort_values(['start', 'end'])
        super().__init__(users, node_coordinates)


class TelecomUserFactory(UserFactory):

    def __init__(self, dataset_dir: str, node_coordinates: np.array):
        self.dataset_dir = pathlib.Path(dataset_dir)
        users = pd.read_excel(self.dataset_dir.joinpath("data_6.1~6.15.xlsx"),
                              usecols=['start time', 'end time', 'latitude', 'longitude', 'user id'],
                              parse_dates=['start time', 'end time'],
                              date_format="%Y-%m-%d %H:%M:%S", )
        users = users[~(users.isna().sum(axis=1).astype(bool))]
        users['user id'] = users['user id'].astype('category').cat.codes
        users = users.rename(
            columns={'start time': 'start', 'end time': 'end', 'user id': 'id', 'latitude': 'lat', 'longitude': 'long'})
        normalized_cabs = (users - users.min()) / (users.max() - users.min())
        users['lat'] = normalized_cabs['lat'].astype(float)
        users['long'] = normalized_cabs['long'].astype(float)
        users['start'] = normalized_cabs['start'].astype(float)
        users['end'] = normalized_cabs['end'].astype(float)
        # Normalize towards uniform distribution
        cols = ['lat', 'long']
        scaler = QuantileTransformer()
        users[cols] = scaler.fit_transform(users[cols])
        users = users.sort_values(['start', 'end'])
        super().__init__(users, node_coordinates)