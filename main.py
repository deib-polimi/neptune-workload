import re
import pandas as pd

# cabspotting

cabs_df = pd.read_csv("cabspottingdata/_cabs.txt", header=None)
cabs_df.columns = ['row']
cabs_df['file'] = cabs_df['row'].map(lambda x: re.findall('"([^"]*)"', x)[0])
cabs_df['updates'] = cabs_df['row'].map(lambda x: re.findall('"([^"]*)"', x)[1])
cabs_df = cabs_df.drop(columns=['row'])

taxi_df = pd.read_csv("cabspottingdata/new_enyenewl.txt", sep=" ", header=None)
taxi_df.columns = ['lat', 'long', 'state', 'timestamp']
taxi_df = taxi_df.drop(columns='state')
print(taxi_df)

# tdrive
cabs = 10357
taxi_df = pd.read_csv("tdrive/release/taxi_log_2008_by_id/1.txt", sep=",", header=None, index_col=0)
taxi_df.columns = ['timestamp', 'lat', 'long']
print(taxi_df)