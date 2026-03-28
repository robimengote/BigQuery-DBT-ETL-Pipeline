import os
import pandas as pd
from google.cloud import storage
from utils import raw_report_transform, split_good_and_bad, upload_parquet_to_gcs_amantes, load_parquet_to_bigquery_amantes_etl, archive_files_new, load_parquet_to_bigquery_quarantine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def bucket_to_tmp(**context):
    bucket_name = os.environ["AMANTES_GCS_BUCKET"]
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # 1. Grab the logical date for this specific Airflow run (Format: YYYY-MM-DD)
    logical_date = context['ds'] 
    logical_date_obj = datetime.strptime(logical_date, "%Y-%m-%d")
    logical_date_stamp = logical_date_obj - timedelta(days=1)
    logical_date_stamp_str = logical_date_stamp.strftime("%Y-%m-%d")

    # 2. Construct the exact target filename 
    expected_filename = f"Report Amante s_{logical_date_stamp_str}.xlsx"
    gcs_path = f"incoming/{expected_filename}"

    blob = bucket.blob(gcs_path)

    # 3. Graceful exit if the shop was closed or no file was uploaded that day
    if not blob.exists():
        print(f"✅ No file found for {logical_date_stamp_str}. Skipping run.")
        context["ti"].xcom_push(key="push_good", value=None)
        context["ti"].xcom_push(key="push_quarantine", value=None)
        return

    # 4. Download just the single file 
    local_file_path = f"/tmp/{expected_filename}"
    blob.download_to_filename(local_file_path)
    print(f"✅ Downloaded {expected_filename}")

    # 5. Transform the single file
    df = raw_report_transform(local_file_path, expected_filename)
    if df is None:
        print(f"⚠️ Skipping {expected_filename} - Transform returned None")
        context["ti"].xcom_push(key="push_good", value=None)
        context["ti"].xcom_push(key="push_quarantine", value=None)
        return

    df_good, df_bad = split_good_and_bad(df)

    # ── Good rows ────────────────────────────────────────
    if len(df_good) > 0:
        df_good['source_file'] = expected_filename
        
        # Save with the date stamped in the Parquet filename to prevent concurrency clashes
        good_path = f"/tmp/final_good_df_{logical_date_stamp_str}.parquet"
        df_good.to_parquet(good_path, index=False)
        
        context["ti"].xcom_push(key="push_good", value=good_path)
        print(f"✅ Good rows saved: {len(df_good)}")
    else:
        context["ti"].xcom_push(key="push_good", value=None)
        print("⚠️ No good rows to process")

    # ── Quarantine rows ──────────────────────────────────
    if len(df_bad) > 0:
        df_bad['source_file'] = expected_filename
        
        quarantine_path = f"/tmp/final_quarantine_df_{logical_date_stamp_str}.parquet"
        df_bad.to_parquet(quarantine_path, index=False)
        
        context["ti"].xcom_push(key="push_quarantine", value=quarantine_path)
        print(f"✅ Quarantine rows saved: {len(df_bad)}")
    else:
        context["ti"].xcom_push(key="push_quarantine", value=None)
        print("✅ No quarantine rows today")


def upload(**context):
    pull_good = context["ti"].xcom_pull(task_ids="bucket_to_tmp", key="push_good")
    pull_bad = context["ti"].xcom_pull(task_ids="bucket_to_tmp", key="push_quarantine")


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
        "pos_data.temp_fact_sales_quarantine"
    )
    print("Rows up for quarantine successfully loaded to fact_sales_quarantine")



def archive(**context):
    bucket_name = os.environ.get('AMANTES_GCS_BUCKET')

    archv_logical_date = context['ds'] 
    archv_logical_date_obj = datetime.strptime(archv_logical_date, "%Y-%m-%d")
    archv_logical_date_stamp = archv_logical_date_obj - timedelta(days=1)
    archv_logical_date_stamp_str = archv_logical_date_stamp.strftime("%Y-%m-%d")

    archv_target_file = f"incoming/Report Amante s_{archv_logical_date_stamp_str}.xlsx"
    target_raw_file = archv_target_file.split("/")[-1]
    
    try:
       archive_files_new(archv_target_file, bucket_name, archv_logical_date_stamp_str)

    except Exception as e:
        print("The archive_files_new function caught an error")


    try:
        archive_files_new('staging/final_good_df.parquet', bucket_name, archv_logical_date_stamp_str)

    except Exception as e:
        print("The archive_files_new function caught an error")

    try:
        archive_files_new('quarantine/final_bad_df.parquet', bucket_name, archv_logical_date_stamp_str)

    except Exception as e:
        print("The archive_files_new function caught an error")



with DAG(
    dag_id='amantes_etl',
    start_date=datetime(2026, 3, 23),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1
) as dag:
    
    bucket_to_docker = PythonOperator(
        task_id='bucket_to_tmp',
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

    trigger_dedup_merge_quarantine = BigQueryInsertJobOperator(
        task_id='call_quarantine_merge_procedure',
        configuration={
            "query": {
                "query": "CALL `pos_data.dedup_temp_fact_sales_quarantine`();",
                "useLegacySql": False,
            }
        }
    )

    archiving = PythonOperator(
        task_id='archive_task',
        python_callable=archive
    )


    bucket_to_docker >> upload_splitted >> bigquery >> trigger_dedup_merge >> trigger_dedup_merge_quarantine >> archiving



