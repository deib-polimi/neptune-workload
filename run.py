import time
import psycopg2
import argparse
import os


def save(run_name):
    conn = psycopg2.connect(
        host="metrics-database.kube-system.svc.cluster.local",
        user="user",
        password="password")
    cur = conn.cursor()
    with open(f'{run_name}_proxy_metric.csv', 'w') as f:
        cur.copy_expert("copy (select * from proxy_metric) to stdout with csv header", f)
        conn.commit()
    with open(f'{run_name}_resource.csv', 'w') as f:
        cur.copy_expert("copy (select * from resource) to stdout with csv header", f)
        conn.commit()
    with open(f'{run_name}_pod_log.csv', 'w') as f:
        cur.copy_expert("copy (select * from pod_log) to stdout with csv header", f)
        conn.commit()
    with open(f'{run_name}_podscale_log.csv', 'w') as f:
        cur.copy_expert("copy (select * from podscale_log) to stdout with csv header", f)
        conn.commit()

def reset():
    conn = psycopg2.connect(
        host="metrics-database.kube-system.svc.cluster.local",
        user="user",
        password="password")
    cur = conn.cursor()
    cur.execute("delete from podscale_log")
    conn.commit()
    cur.execute("delete from pod_log")
    conn.commit()
    cur.execute("delete from proxy_metric")
    conn.commit()
    cur.execute("delete from resource")
    conn.commit()

def run_exp(run, run_id):
    reset()
    host = f"http://{run}.openfaas-fn.svc.cluster.local"
    mode = f"{run.replace('-', '_')}"
    if mode == "video_processing" or mode == "image_recognition":
        users = 15
    elif mode == "dynamic_html" or mode == "compression" or mode == "thumbnailer":
        users = 50
    else:
        users = 25
    command = f"locust -f workload.py --headless --host {host} --duration 1200 --mode {mode} --period 1200 --start 60 --min_users 1 --max_users {users}"
    os.system(command)
    time.sleep(120)
    save(f"{run}_{run_id}")

runs = [
    "compression",
    "dynamic-html",
    "graph-bfs",
    "graph-mst",
    "pagerank",
    "thumbnailer",
    "video-processing"
]

for run in runs:
    for run_id in range(5):
        run_exp(run, run_id)