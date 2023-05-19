#  Eka A Kurniawan
#  Gustaf Pandu Pranata
#  Rendo zenico

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import pytz

default_args = {
    'start_date': datetime(2023, 5, 3, 4, 0, tzinfo=pytz.timezone('Asia/Bangkok')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_anomaly',
    description='ETL Process Data Anomaly Regular Updates',
    schedule_interval='0 4 * * *',  # Schedule to run every day at 4 AM
    default_args=default_args,
)

def process_data(input_file, output_file):
    data_raw = pd.read_csv(input_file, sep=';', dtype=str)

    # Add New Field
    data_raw["metric"] = "SNR"

    # Rename fields
    data_raw.columns = [
        'datex',
        'adn',
        'scp_id',
        'hourx',
        'status_code',
        'value',
        'metric'
    ]

    # Reorder the columns
    data_raw = data_raw[[
        'datex',
        'hourx',
        'adn',
        'scp_id',
        'status_code',
        'metric',
        'value'
    ]]

    data_raw.to_csv(output_file, sep=';', index=False)
    return data_raw

def process_data_anomaly():
    input_file = '/opt/airflow/dags/anomaly_snr.csv'
    output_file = '/opt/airflow/dags/anomaly_snr_update.csv'
    processed_data = process_data(input_file, output_file)
    print(processed_data.head())
    
def export_to_csv():
    input_file = '/opt/airflow/dags/anomaly_snr.csv'
    output_file = '/opt/airflow/dags/anomaly_snr_update.csv'
    process_data(input_file, output_file)
    
def import_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    connection = pg_hook.get_conn()
    crsr = connection.cursor()

    crsr.execute("TRUNCATE TABLE tb_anomaly_temp")
    rows_deleted = crsr.rowcount
    print(rows_deleted)

    with open('/opt/airflow/dags/anomaly_snr_update.csv', 'r') as file:
        next(file)
        crsr.copy_from(file, 'tb_anomaly_temp', sep=';')

    print("Total inserted to table temp: ", crsr.rowcount)

    crsr.execute("INSERT INTO tb_anomaly \
                  SELECT datex,hourx,adn,scp_id,status_code,metric,value FROM tb_anomaly_temp \
                  WHERE datex between date_trunc('month',CURRENT_DATE) AND  NOW() \
                  group by datex,hourx,adn,scp_id,status_code,metric,value \
                  ORDER BY datex,hourx \
                  ON CONFLICT(datex,hourx,adn,scp_id,status_code,metric) DO UPDATE \
                  SET value=excluded.value")
    print("Total inserted to table anomaly: ", crsr.rowcount)
    crsr.close()
    connection.commit()
    connection.close()

with dag:
    task_process_data_anomaly = PythonOperator(
    task_id='process_data_anomaly',
    python_callable=process_data_anomaly,
    )
    
    task_export_to_csv = PythonOperator(
    task_id='export_to_csv',
    python_callable=export_to_csv,
    )

    task_import_to_postgres = PythonOperator(
    task_id='import_to_postgres',
    python_callable=import_to_postgres,
    )
    
task_process_data_anomaly >> task_export_to_csv >> task_import_to_postgres

