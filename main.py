from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# defining dag argument
default_args = {
    "owner": "mide clp",
    "start_date": days_ago(0),
    "email": "a2pdigital.com@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# defining the dag
dag = DAG(
    "ETL_toll_data",
    default_args=default_args,
    schedule_interval= timedelta(days=1),
    description="Apache Airflow Final Assignment",
)

# defining the first task

unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command = "tar -xzf $AIRFLOW_HOME/dags/finalassignment/tolldata.tgz",
    dag=dag,
)

# defining the second task
extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command="cut -d',' -f1-4 $AIRFLOW_HOME/dags/finalassignment/vehicle-data.csv > $AIRFLOW_HOME/dags/finalassignment/csv_data.csv",
    dag=dag,
)

# defining the third task
extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command="cut  -f5-7 $AIRFLOW_HOME/dags/finalassignment/tollplaza-data.tsv | tr '\t'  ',' > $AIRFLOW_HOME/dags/finalassignment/tsv_data.csv",
    dag=dag,
)

# defining the fourth task
extract_data_from_fixed_width = BashOperator(
    task_id = "extract_data_from_fixed_width",
    bash_command="sed -e 's/./&,/6' -e 's/./&,/32' -e 's/./&,/41' -e 's/./&,/50' \
        -e 's/./&,/61' -e 's/./&,/66' $AIRFLOW_HOME/dags/finalassignment/payment-data.txt | cut -d',' -f6,7 > $AIRFLOW_HOME/dags/finalassignment/fixed_width_data.csv",
    dag=dag,
)

# defining the fifth task
consolidate_data = BashOperator(
    task_id = "consolidate_data",
    bash_command="paste -d',' $AIRFLOW_HOME/dags/finalassignment/csv_data.csv $AIRFLOW_HOME/dags/finalassignment/tsv_data.csv $AIRFLOW_HOME/dags/finalassignment/fixed_width_data.csv > $AIRFLOW_HOME/dags/finalassignment/extracted_data.csv",
    dag=dag,

)

# defining the sixth task
transform_data = BashOperator(
    task_id = "transform_data",
    bash_command="awk -F',' '{ print $1\",\"$2\",\"$3\",\"toupper($4)\",\"$5\",\"$6\",\"$7; }' < $AIRFLOW_HOME/dags/finalassignment/extracted_data.csv\
          > $AIRFLOW_HOME/dags/finalassignment/staging/transformed_data.csv",
    dag=dag,

)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
