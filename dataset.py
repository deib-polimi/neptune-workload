import pathlib
import re

import numpy as np
import pandas as pd


class User:

    def __init__(self):
        pass

    def get_position(self, timestamp: int):
        pass


class CabspottingUserFactory:

    def __init__(self, dataset_dir: str):
        self.dataset_dir = pathlib.Path(dataset_dir)
        cabs_df = pd.read_csv(self.dataset_dir.joinpath("_cabs.txt"), header=None)
        cabs_df.columns = ['row']
        cabs = cabs_df['row'].map(lambda x: re.findall('"([^"]*)"', x)[0]).to_list()
        cabs_info = []
        for i, cab_id in enumerate(cabs):
            cab_df = pd.read_csv(self.dataset_dir.joinpath(f"new_{cab_id}.txt"), sep=" ", header=None,
                                 names=['lat', 'long', 'state', 'timestamp'], usecols=['lat', 'long', 'timestamp'])
            cab_df['id'] = i
            cabs_info.append(cab_df)
        self.cabs = pd.concat(cabs_info)
        normalized_cabs = (self.cabs - self.cabs.min()) / (self.cabs.max() - self.cabs.min())
        self.cabs['lat'] = normalized_cabs['lat']
        self.cabs['long'] = normalized_cabs['long']
        self.cabs['timestamp'] = normalized_cabs['timestamp']


class TDriveUserFactory:

    def __init__(self, dataset_dir: str):
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
            cab_df['timestamp'] = cab_df['timestamp']
            cabs_info.append(cab_df)
        self.cabs = pd.concat(cabs_info)
        normalized_cabs = (self.cabs - self.cabs.min()) / (self.cabs.max() - self.cabs.min())
        self.cabs['lat'] = normalized_cabs['lat'].astype(float)
        self.cabs['long'] = normalized_cabs['long'].astype(float)
        self.cabs['timestamp'] = normalized_cabs['timestamp'].astype(float)


class TelecomUserFactory:

    def __init__(self, dataset_dir: str):
        self.dataset_dir = pathlib.Path(dataset_dir)
        self.users = pd.read_excel(self.dataset_dir.joinpath("data_6.1~6.15.xlsx"),
                                  usecols=['start time', 'end time', 'latitude', 'longitude', 'user id'],
                                  parse_dates=['start time', 'end time'],
                                  date_format="%Y-%m-%d %H:%M:%S", )
        self.users = self.users[~(self.users.isna().sum(axis=1).astype(bool))]
        self.users['user id'] = self.users['user id'].astype('category').cat.codes
        self.users = self.users.rename(columns={'start time': 'start', 'end time': 'end', 'user id': 'id', 'latitude': 'lat', 'longitude': 'long'})
        normalized_cabs = (self.users - self.users.min()) / (self.users.max() - self.users.min())
        self.users['lat'] = normalized_cabs['lat'].astype(float)
        self.users['long'] = normalized_cabs['long'].astype(float)
        self.users['start'] = normalized_cabs['start'].astype(float)
        self.users['end'] = normalized_cabs['end'].astype(float)

        # cabs_info = []
        # for i in range(1, 10357):
        #     file = self.dataset_dir.joinpath(f"release/taxi_log_2008_by_id/{i}.txt")
        #     if file.stat().st_size == 0:
        #         continue
        #     cab_df = pd.read_csv(file, sep=",", header=None, index_col=0, parse_dates=['timestamp'],
        #                          date_format="%Y-%m-%d %H:%M:%S", names=['timestamp', 'lat', 'long'])
        #     cab_df.columns = ['timestamp', 'lat', 'long']
        #     cab_df['cab_id'] = i
        #     cab_df['timestamp'] = cab_df['timestamp']
        #     cabs_info.append(cab_df[:100])
        # self.cabs = pd.concat(cabs_info)
        # normalized_cabs = (self.cabs - self.cabs.min()) / (self.cabs.max() - self.cabs.min())
        # self.cabs['lat'] = normalized_cabs['lat'].astype(float)
        # self.cabs['long'] = normalized_cabs['long'].astype(float)
        # self.cabs['timestamp'] = normalized_cabs['timestamp'].astype(float)


if __name__ == '__main__':
    # user_factory = CabspottingUserFactory("cabspottingdata")
    # user_factory = TDriveUserFactory("tdrive")
    user_factory = TelecomUserFactory("telecom")
    pass
