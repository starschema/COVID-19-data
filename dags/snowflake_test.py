# -*- coding: utf-8 -*-
# Copyright 2019 Dineshkarthik Raveendran

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example DAG demonstrating the usage of the SnowflakeOperator & Hook."""
import logging

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "admin", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="snowflake_test", default_args=args, schedule_interval=None
)

create_insert_query = [
    """create table public.test_table (amount number);""",
    """insert into public.test_table values(1),(2),(3);""",
]


def row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="OJ10999_COVID19")
    result = dwh_hook.get_first("select count(*) from public.test_table")
    logging.info("Number of rows in `public.test_table`  - %s", result[0])


with dag:
    create_insert = SnowflakeOperator(
        task_id="snowfalke_create",
        sql=create_insert_query,
        snowflake_conn_id="OJ10999_COVID19",
    )

    get_count = PythonOperator(task_id="get_count", python_callable=row_count)
create_insert >> get_count
