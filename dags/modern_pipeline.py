import os
import pandas as pd
from google.cloud import storage
from utils import raw_report_transform, split_good_and_bad, upload_parquet_to_gcs_amantes, load_parquet_to_bigquery_amantes_etl, archive_files_new, load_parquet_to_bigquery_quarantine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def bucket_to_tmp(**context):
    bucket_name = os.environ["AMANTES_GCS_BUCKET"]
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix="incoming/"))

    if not blobs:
        raise ValueError("No files found in GCS /incoming/")

    good_df_list = []	
    quarantine_df_list = []

    for file in blobs:
        filename = file.name.split("/")[-1]
        file_path = f"/tmp/{filename}"
        file.download_to_filename(file_path)
        print(f"✅ Downloaded {filename}")

        df = raw_report_transform(file_path, filename)
        if df is None:
            print(f"⚠️ Skipping {filename}")
            continue

        df_good, df_bad = split_good_and_bad(df)
       
        if len(df_good) > 0:
            df_good['source_file'] = filename
            good_df_list.append(df_good)
            manila_time_stamp = pd.to_datetime(df_good['payment_time']).dt.date.min()
            context["ti"].xcom_push(key='processing_date', value=str(manila_time_stamp))

        else:
            print(f"No good rows found in {filename}")

        if len(df_bad) > 0:
            df_bad['source_file'] = filename
            quarantine_df_list.append(df_bad)
        else:
            print(f"No quarantine rows found in {filename}")

    # ── Good rows ────────────────────────────────────────
    if len(good_df_list) > 0:
        final_good = pd.concat(good_df_list, ignore_index=True)
        good_path = "/tmp/final_good_df.parquet"
        final_good.to_parquet(good_path, index=False)
        context["ti"].xcom_push(key="push_good", value=good_path)
        print(f"✅ Good rows saved: {len(final_good)}")
    else:
        context["ti"].xcom_push(key="push_good", value=None)
        print("⚠️ No good rows to process")

    # ── Quarantine rows ──────────────────────────────────
    if len(quarantine_df_list) > 0:
        final_quarantine = pd.concat(quarantine_df_list, ignore_index=True)
        quarantine_path = "/tmp/final_quarantine_df.parquet"
        final_quarantine.to_parquet(quarantine_path, index=False)
        context["ti"].xcom_push(key="push_quarantine", value=quarantine_path)
        print(f"✅ Quarantine rows saved: {len(final_quarantine)}")
    else:
        context["ti"].xcom_push(key="push_quarantine", value=None)
        print("✅ No quarantine rows today")


def upload(**context):
    pull_good = context["ti"].xcom_pull(task_ids="bucket_to_temp_docker", key="push_good")
    pull_bad = context["ti"].xcom_pull(task_ids="bucket_to_temp_docker", key="push_quarantine")


    if pull_good is None:
        print("⚠️ No good rows to upload")
    else:
        upload_parquet_to_gcs_amantes(pull_good, "staging/final_good_df.parquet")
        print(f"Sucessfully uploaded good rows to GCS Staging")

    if pull_bad is None:
        print("No quarantine rows to upload")
    else:
        upload_parquet_to_gcs_amantes(pull_bad, "quarantine/final_bad_df.parquet")
        print(f"Sucessfully uploaded bad rows to GCS Quarantine")



def bucket_to_bigquery(**context):
    bucket_name = os.environ["AMANTES_GCS_BUCKET"]
    load_parquet_to_bigquery_amantes_etl(f"gs://{bucket_name}/staging/final_good_df.parquet", "pos_data.temp_fact_sales")


    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob("quarantine/final_bad_df.parquet")

    if not blob.exists():
        print("✅ No quarantine file in GCS today")
        return

    load_parquet_to_bigquery_quarantine(
        f"gs://{bucket_name}/quarantine/final_bad_df.parquet",
        "pos_data.fact_sales_quarantine"
    )
    print("Rows up for quarantine successfully loaded to fact_sales_quarantine")



def archive(**context):
    bucket_name = os.environ.get('AMANTES_GCS_BUCKET')
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_path = list(bucket.list_blobs(prefix='incoming/'))

    for file in source_path:
        try:
            archive_files_new(file.name, bucket_name)
            print(f"Successfully archived {file.name}")
        except Exception as e:
            print(f"{e}")

    try:
        archive_files_new('staging/final_good_df.parquet', bucket_name)
        print("Sucessfully archived good rows")
    except Exception as e:
        print(f"{e}")

    try:
        archive_files_new('quarantine/final_bad_df.parquet', bucket_name)
        print("Successfully archived bad rows")
    except Exception as e:
        print(f"{e}")



with DAG(
    dag_id='amantes_etl',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    bucket_to_docker = PythonOperator(
        task_id='bucket_to_temp_docker',
        python_callable=bucket_to_tmp
    )

    upload_splitted = PythonOperator(
        task_id='upload_splitted',
        python_callable=upload
    )

    bigquery = PythonOperator(
        task_id='bigquery',
        python_callable=bucket_to_bigquery
    )

    trigger_dedup_merge = BigQueryInsertJobOperator(
        task_id='call_staging_merge_procedure',
        configuration={
            "query": {
                "query": "CALL `pos_data.dedup_temp_fact_sales`();",
                "useLegacySql": False,
            }
        }
    )

    archiving = PythonOperator(
        task_id='archive_task',
        python_callable=archive
    )


    bucket_to_docker >> upload_splitted >> bigquery >> trigger_dedup_merge >> archiving



