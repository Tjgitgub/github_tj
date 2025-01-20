"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.2.16, generation date: 2025/01/20 15:04:37
DV_NAME: moto_scn01 - Release: 2(2) - Comment: 2 - Release date: 2025/01/20 10:43:14, 
SRC_NAME: moto_mktg_scn01 - Release: moto_mktg_scn01(1) - Comment: VaultSpeed automated setup - Release date: 2025/01/16 14:53:46
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
	"start_date":datetime.strptime("17-01-2025 07:00:00", "%d-%m-%Y %H:%M:%S")
}

path_to_mtd = Path(Variable.get("path_to_metadata"))

if (path_to_mtd / "19_mappings_mktg_incr_fl_20250120_150437.json").exists():
	with open(path_to_mtd / "19_mappings_mktg_incr_fl_20250120_150437.json") as file: 
		mappings = json.load(file)

else:
	with open(path_to_mtd / "mappings_mktg_incr_fl.json") as file: 
		mappings = json.load(file)

mktg_incr_fl = DAG(
	dag_id="mktg_incr_fl", 
	default_args=default_args,
	description="mktg_incr_fl", 
	schedule_interval="@daily", 
	concurrency=4, 
	catchup=False, 
	max_active_runs=1,
	tags=["VaultSpeed", "mm", "moto_scn01"]
)

# Create incremental fmc tasks
# insert load metadata
fmc_mtd = JdbcOperator(
	task_id="fmc_mtd", 
	jdbc_conn_id="dv", 
	sql=f"""select "moto_scn01_proc"."set_fmc_mtd_fl_incr_mm"(p_dag_name := '{{{{ dag_run.dag_id }}}}', p_load_cycle_id := '{{{{ dag_run.id }}}}', p_load_date := '{{{{ data_interval_end.strftime(\"%Y-%m-%d %H:%M:%S.%f\") }}}}');""", 
	dag=mktg_incr_fl
)

tasks = {"fmc_mtd":fmc_mtd}

# Create mapping tasks
for map, info in mappings.items():
	task = JdbcOperator(
		task_id=map, 
		jdbc_conn_id="dv", 
		sql=f"""select {info["map_schema"]}."{map}"();""", 
		dag=mktg_incr_fl
	)
	
	for dep in info["dependencies"]:
		task << tasks[dep]
	
	tasks[map] = task
	

# task to indicate the end of a load
end_task = DummyOperator(
	task_id="end_of_load", 
	dag=mktg_incr_fl
)

# Analyze tables
if (path_to_mtd / "19_FL_mtd_mktg_incr_fl_20250120_150437.json").exists():
	with open(path_to_mtd / "19_FL_mtd_mktg_incr_fl_20250120_150437.json") as file: 
		analyze_data = json.load(file)
else:
	with open(path_to_mtd / "FL_mtd_mktg_incr_fl.json") as file: 
		analyze_data = json.load(file)

for table, data in analyze_data.items():
	task = JdbcOperator(
		task_id=f"analyze_{table}", 
		jdbc_conn_id="dv", 
		sql=f"""analyse "{data["schema"]}"."{table}";""", 
		dag=mktg_incr_fl
	)
	
	for dep in data["dependencies"]:
		task << tasks[dep]
	end_task << task

# Save load status tasks
fmc_load_fail = JdbcOperator(
	task_id="fmc_load_fail", 
	jdbc_conn_id="dv", 
	sql=f"""select "moto_scn01_proc"."fmc_upd_run_status_fl_mm"(p_load_cycle_id := '{{{{ dag_run.id }}}}', p_success_flag := '0');""", 
	trigger_rule="one_failed", 
	dag=mktg_incr_fl
)
fmc_load_fail << end_task

fmc_load_success = JdbcOperator(
	task_id="fmc_load_success", 
	jdbc_conn_id="dv", 
	sql=f"""select "moto_scn01_proc"."fmc_upd_run_status_fl_mm"(p_load_cycle_id := '{{{{ dag_run.id }}}}', p_success_flag := '1');""", 
	dag=mktg_incr_fl
)
fmc_load_success << end_task

