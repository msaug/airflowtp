import airflow
import datetime
import urllib.request as request
import pandas as pd
import json
from faker import Faker
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator


fake = Faker()

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dnd_dag = DAG(
    dag_id='dnd_dag',
    default_args=default_args_dict,
    catchup=False,
)

##text=List of strings to be written to file


# generate 5 random names from faker
def _generate_names(output_folder):
    data = {
        'names':[],
    }
    for i in range(5):
        data.names.append(fake.name())
    print(data)
    with open(output_folder,'w') as file:
       for line in names:
           file.write(line)
           file.write('\n')

    json_string = json.dumps(names)
    print(json_string)
    with open(f"{output_folder}.json", 'w') as outfile:
        json.dump(json_string, outfile)


def _set_attributes(input_folder,output_folder):
    file = open(input_folder, "r")
    csv_reader = csv.reader(file)

    lists_from_csv = []
    for row in csv_reader:
        lists_from_csv.append(row)

    print(lists_from_csv)



#Use the same db as airflow, or use a csv file
#Reuse the same db as airflow, but in real world we would use another one


task_one = PythonOperator(
    task_id='generate_names',
    dag=dnd_dag,
    python_callable=_generate_names,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/names.csv",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# oh noes :( it's xlsx... let's make it a csv.
#
# task_two = BashOperator(
#     task_id='transmute_to_csv',
#     dag=dnd_dag,
#     bash_command="xlsx2csv /opt/airflow/dags/{{ds_nodash}}.xlsx > /opt/airflow/dags/{{ds_nodash}}_correct.csv",
# )
#
# task_three = BashOperator(
#     task_id='time_filter',
#     dag=dnd_dag,
#     bash_command="awk -F, 'int($31) > 1588612377' /opt/airflow/dags/{{ds_nodash}}_correct.csv > /opt/airflow/dags/{{ds_nodash}}_correct_filtered.csv",
# )
#
# task_four = BashOperator(
#     task_id='load',
#     dag=dnd_dag,
#     bash_command="echo \"done\""
# )
#
# task_five = BashOperator(
#     task_id='cleanup',
#     dag=dnd_dag,
#     bash_command="rm /opt/airflow/dags/{{ds_nodash}}_correct.csv /opt/airflow/dags/{{ds_nodash}}_correct_filtered.csv /opt/airflow/dags/{{ds_nodash}}.xlsx",
# )
#
# task_one >> task_two >> task_three >> task_four >> task_five
