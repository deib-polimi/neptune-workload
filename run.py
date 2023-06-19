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

workloads = [
    "cabspotting",
    "tdrive",
    "telecom",
]

def run_exp(workload, run_id):
    reset()
    command = f"locust -f locust_workload.py --headless --host www.google.com --duration 1800 --workload {workload}"
    os.system(command)
    time.sleep(120)
    save(f"{workload}_{run_id}")

for workload in workloads:
    for run_id in range(3):
        run_exp(workload, run_id)