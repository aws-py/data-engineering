from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrStepSensor
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

# Configuración básica del DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    'bronze_to_gold_workflow',
    default_args=default_args,
    description='ETL workflow from Bronze to Gold layer using PySpark on EMR',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['bronze', 'silver', 'gold', 'pyspark', 'airflow']
) as dag:

    # Dummy para marcar el inicio
    start = DummyOperator(task_id='start')

    # ID del clúster EMR
    cluster_id = 'j-XXXXXXXXXXXXX'  # Reemplazar con el ID del clúster EMR

    # Pasos para el procesamiento en PySpark
    spark_steps = [
        {
            'Name': 'Transform Bronze to Silver',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    's3://path-to-your-scripts/bronze_to_silver.py',
                    '--input', 's3://bronze-layer-bucket/',
                    '--output', 's3://silver-layer-bucket/'
                ],
            },
        },
        {
            'Name': 'Transform Silver to Gold',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    's3://path-to-your-scripts/silver_to_gold.py',
                    '--input', 's3://silver-layer-bucket/',
                    '--output', 's3://gold-layer-bucket/'
                ],
            },
        },
    ]

    # Añadir pasos al clúster EMR
    add_steps = EmrAddStepsOperator(
        task_id='add_spark_steps',
        job_flow_id=cluster_id,
        steps=spark_steps
    )

    # Sensores para los pasos
    wait_for_silver = EmrStepSensor(
        task_id='wait_for_bronze_to_silver',
        job_flow_id=cluster_id,
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[0] }}"
    )

    wait_for_gold = EmrStepSensor(
        task_id='wait_for_silver_to_gold',
        job_flow_id=cluster_id,
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps', key='return_value')[1] }}"
    )

    # Dummy para marcar el fin
    end = DummyOperator(task_id='end')

    # Definición del flujo
    start >> add_steps >> wait_for_silver >> wait_for_gold >> end
