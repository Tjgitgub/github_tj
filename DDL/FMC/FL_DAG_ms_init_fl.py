"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.13, generation date: 2025/01/13 12:20:53
DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 fmc y - Release date: 2025/01/13 12:05:56, 
SRC_NAME: moto_sales_scn01 - Release: moto_sales_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/13 09:07:41
 """


from datetime import datetime, timedelta
from pathlib import Path
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from vs_fmc_plugin.operators.jdbc_operator import JdbcOperator


default_args = {
	"owner":"Vaultspeed",
	"retries": 3,
	"retry_delay": timedelta(seconds=10),
	"start_date":datetime.strptime("12-01-2025 12:00:00", "%d-%m-%Y %H:%M:%S")
}

path_to_mtd = Path(Variable.get("path_to_metadata"))

if (path_to_mtd / "6_mappings_ms_init_fl_20250113_122053.json").exists():
	with open(path_to_mtd / "6_mappings_ms_init_fl_20250113_122053.json") as file: 
		mappings = json.load(file)

else:
	with open(path_to_mtd / "mappings_ms_init_fl.json") as file: 
		mappings = json.load(file)

ms_init_fl = DAG(
	dag_id="ms_init_fl", 
	default_args=default_args,
	description="ms_init_fl", 
	schedule_interval="@once", 
	concurrency=4, 
	catchup=False, 
	max_active_runs=1,
	tags=["VaultSpeed", "ms", "moto_scn01"]
)

# Create initial fmc tasks
# insert load metadata
fmc_mtd = JdbcOperator(
	task_id="fmc_mtd", 
	jdbc_conn_id="dv", 
	sql=f"""select "moto_scn01_proc"."set_fmc_mtd_fl_init_ms"(p_dag_name := '{{{{ dag_run.dag_id }}}}', p_load_cycle_id := '{{{{ dag_run.id }}}}', p_load_date := '{{{{ execution_date.strftime(\"%Y-%m-%d %H:%M:%S.%f\") }}}}');""", 
	dag=ms_init_fl
)

tasks = {"fmc_mtd":fmc_mtd}

# Create mapping tasks
for map, info in mappings.items():
	task = JdbcOperator(
		task_id=map, 
		jdbc_conn_id="dv", 
		sql=f"""select {info["map_schema"]}."{map}"();""", 
		dag=ms_init_fl
	)
	
	for dep in info["dependencies"]:
		task << tasks[dep]
	
	tasks[map] = task
	

# task to indicate the end of a load
end_task = DummyOperator(
	task_id="end_of_load", 
	dag=ms_init_fl
)

# Analyze tables
if (path_to_mtd / "6_FL_mtd_ms_init_fl_20250113_122053.json").exists():
	with open(path_to_mtd / "6_FL_mtd_ms_init_fl_20250113_122053.json") as file: 
		analyze_data = json.load(file)
else:
	with open(path_to_mtd / "FL_mtd_ms_init_fl.json") as file: 
		analyze_data = json.load(file)

for table, data in analyze_data.items():
	task = JdbcOperator(
		task_id=f"analyze_{table}", 
		jdbc_conn_id="dv", 
		sql=f"""analyse "{data["schema"]}"."{table}";""", 
		dag=ms_init_fl
	)
	
	for dep in data["dependencies"]:
		task << tasks[dep]
	end_task << task

# Save load status tasks
fmc_load_fail = JdbcOperator(
	task_id="fmc_load_fail", 
	jdbc_conn_id="dv", 
	sql=f"""select "moto_scn01_proc"."fmc_upd_run_status_fl_ms"(p_load_cycle_id := '{{{{ dag_run.id }}}}', p_success_flag := '0');""", 
	trigger_rule="one_failed", 
	dag=ms_init_fl
)
fmc_load_fail << end_task

fmc_load_success = JdbcOperator(
	task_id="fmc_load_success", 
	jdbc_conn_id="dv", 
	sql=f"""select "moto_scn01_proc"."fmc_upd_run_status_fl_ms"(p_load_cycle_id := '{{{{ dag_run.id }}}}', p_success_flag := '1');""", 
	dag=ms_init_fl
)
fmc_load_success << end_task

