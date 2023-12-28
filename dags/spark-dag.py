from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_airflow",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Batch Processing with PySpark",
    start_date=days_ago(1),
)

readData = SparkSubmitOperator(
    application="/spark-scripts/spark-readData.py",
    conn_id="spark_tgs",
    conf={"spark.jars.packages": "org.postgresql:postgresql:42.2.18"},
    task_id="Load-Data",
    dag=spark_dag,
)

readData