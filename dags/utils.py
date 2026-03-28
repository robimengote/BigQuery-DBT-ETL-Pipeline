import pandas as pd
import re
import os
from google.cloud import storage, bigquery
from datetime import datetime
import pytz
from product_mappings import product_to_sub_category, sub_category_to_category, product_corrections


# -----------------------------------------
# TRANSFORM
# -----------------------------------------

def raw_report_transform(file_content, filename):
    """
    Cleans and transforms raw POS Excel report into a structured DataFrame.
    Returns cleaned DataFrame or None if file cannot be read.
    """

    # 1. Load Data
    try:
        df = pd.read_excel(file_content, sheet_name='Paid order list')
        print(f"✅ Loaded {filename} — {len(df)} rows")
    except Exception as e:
        print(f"❌ Skipping {filename}: Could not read Excel. Error: {e}")
        return None

    # 2. Clean Column Names
    try:
        df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace("-", "_").str.lower()
        print("✅ Column names cleaned")
        print(f"   Columns: {list(df.columns)}")
    except Exception as e:
        print(f"❌ Failed to clean column names: {e}")
        raise

    # 3. Explode the Product List
    try:
        df['product_list'] = df['products'].astype(str).str.split(',')
        df_exploded = df.explode('product_list')
        df_exploded = df_exploded[df_exploded['product_list'].str.strip() != '']
        df_exploded['product_list'] = df_exploded['product_list'].str.strip()
        print(f"✅ Products exploded — {len(df_exploded)} rows after explode")
    except Exception as e:
        print(f"❌ Failed to explode product list: {e}")
        raise

    # 4. Extract Size
    try:
        size_pattern = r'(Solo|Duo|Medio|Familia)'
        df_exploded['size'] = df_exploded['product_list'].str.extract(
            size_pattern, flags=re.I, expand=False).str.title()
        print("✅ Size extracted")
    except Exception as e:
        print(f"❌ Failed to extract size: {e}")
        raise

    # 5. Extract Variation (Hot/Cold)
    try:
        hot_cold_pattern = r'(Hot|Cold)'
        df_exploded['variation'] = df_exploded['product_list'].str.extract(
            hot_cold_pattern, flags=re.I, expand=False).str.title()
        print("✅ Variation extracted")
    except Exception as e:
        print(f"❌ Failed to extract variation: {e}")
        raise

    # 6. Extract Flavor (Fries/Lemonade)
    try:
        target_ff_p = r'(Fries|Lemonade)'
        is_target1 = df_exploded['product_list'].str.contains(target_ff_p, case=False, na=False)
        fries_flavor_pattern = r'(Cheese|BBQ|Sour Cream|Plain|Mango)'
        df_exploded.loc[is_target1, 'flavor'] = df_exploded.loc[
            is_target1, 'product_list'].str.extract(
            fries_flavor_pattern, flags=re.I, expand=False).str.title()
        print("✅ Flavor extracted")
    except Exception as e:
        print(f"❌ Failed to extract flavor: {e}")
        raise

    # 7. Extract Sugar Level
    try:
        sugar_pattern = r'(Sugar 20%|Sugar 50%|Sugar 75%|Sugar 100%)'
        df_exploded['sugar_level'] = df_exploded['product_list'].str.extract(
            sugar_pattern, flags=re.I, expand=False).str.title()
        print("✅ Sugar level extracted")
    except Exception as e:
        print(f"❌ Failed to extract sugar level: {e}")
        raise

    # 8. Extract Spice Level
    try:
        spicy_pattern = r'(Mild \(1/4\)|Regular \(2/4\)|Spicy \(3/4\))'
        df_exploded['spice_level'] = df_exploded['product_list'].str.extract(
            spicy_pattern, flags=re.I, expand=False).str.title()
        print("✅ Spice level extracted")
    except Exception as e:
        print(f"❌ Failed to extract spice level: {e}")
        raise

    # 9. Extract Quantity
    try:
        df_exploded['quantity'] = df_exploded['product_list'].str.extract(
            r'x\s*(\d+)').astype(float).fillna(1)
        print("✅ Quantity extracted")
    except Exception as e:
        print(f"❌ Failed to extract quantity: {e}")
        raise

    # 10. Complex Item Name Extraction (Croissant, Croffle, Cookies)
    try:
        target_categories = ['Croissant', 'Croffle', 'Cookies', 'Cookie']
        target_mask_pattern = r'(' + '|'.join(target_categories) + r')'
        flavors_list = [
            'Chip and Chunk Walnut', 'Nutella Pecan Cookie', 'Red Velvet Cookie',
            'Smores Cookie', 'Almond Nutella', 'Biscoff Cookie', 'Strawberry Cream',
            'Spam and Egg', 'Chip and Chunk', 'Biscoff', 'Caramel', 'Chocolate',
            'Matcha', 'Oreo', 'Plain', 'Smores', 'Red Velvet', 'Dubai'
        ]
        flavor_pattern = r'(' + '|'.join(map(re.escape, flavors_list)) + r')'
        is_target = df_exploded['product_list'].str.contains(
            target_mask_pattern, case=False, na=False)

        df_exploded.loc[is_target, 'temp_flavor'] = df_exploded.loc[
            is_target, 'product_list'].str.extract(flavor_pattern, flags=re.I, expand=False)
        df_exploded.loc[is_target, 'temp_flavor'] = df_exploded.loc[
            is_target, 'temp_flavor'].str.replace(
            r'\s*Cookie', '', regex=True, flags=re.I).str.strip()

        df_exploded.loc[is_target, 'category_name'] = df_exploded.loc[
            is_target, 'product_list'].str.extract(
            target_mask_pattern, flags=re.I, expand=False).str.title()
        df_exploded.loc[
            is_target & (df_exploded['category_name'] == 'Cookie'),
            'category_name'] = 'Cookies'

        df_exploded.loc[is_target, 'clean_item'] = (
            df_exploded.loc[is_target, 'category_name'] + " - " +
            df_exploded.loc[is_target, 'temp_flavor']
        )
        print("✅ Complex item names extracted")
    except Exception as e:
        print(f"❌ Failed to extract complex item names: {e}")
        raise

    # 11. Handle Non-Targets
    try:
        df_exploded.loc[~is_target, 'clean_item'] = df_exploded.loc[
            ~is_target, 'product_list'].str.replace(r'x\s*\d+', '', regex=True)
        df_exploded.loc[~is_target, 'clean_item'] = df_exploded.loc[
            ~is_target, 'clean_item'].str.replace(
            r'\s*\(.*\)', '', regex=True).str.strip()
        print("✅ Non-target items cleaned")
    except Exception as e:
        print(f"❌ Failed to handle non-target items: {e}")
        raise

    # 12. Manual Corrections
    try:
        df_exploded['clean_item'] = df_exploded['clean_item'].replace(product_corrections)
        print("✅ Manual corrections applied")
    except Exception as e:
        print(f"❌ Failed to apply manual corrections: {e}")
        raise

    # 13. Map Sub-Categories and Categories
    try:
        df_exploded['sub_category'] = df_exploded['clean_item'].map(product_to_sub_category)
        df_exploded['category'] = df_exploded['sub_category'].map(sub_category_to_category)
        print("✅ Sub-categories and categories mapped")
    except Exception as e:
        print(f"❌ Failed to map categories: {e}")
        raise

    # 14. Payment Type
    try:
        def get_payment_type(row):
            val_cash = str(row.get('cash', 0))
            if val_cash in ('0.00', '0'):
                return 'Free/Voucher/Discounted'
            elif val_cash != '-':
                return 'Cash'
            elif str(row.get('gcash', '-')) != '-':
                return 'Gcash'
            else:
                return 'Credit / Debit'

        df_exploded['payment_type'] = df_exploded.apply(get_payment_type, axis=1)
        print("✅ Payment type mapped")
    except Exception as e:
        print(f"❌ Failed to map payment type: {e}")
        raise

    # 15. Final Cleanup
    try:
        cols_to_use = [
            'order_id', 'clean_item', 'sub_category', 'category', 'flavor',
            'variation', 'size', 'quantity', 'spice_level', 'sugar_level',
            'product_amount', 'received_amount', 'payment_time', 'order_type',
            'payment_type', 'type/channel'
        ]
        existing_cols = [c for c in cols_to_use if c in df_exploded.columns]
        df_exploded = df_exploded[existing_cols]
        df_exploded = df_exploded[df_exploded['clean_item'].astype(str) != 'nan']
        df_exploded['clean_item'] = df_exploded['clean_item'].str.title()

        for col in ['received_amount', 'product_amount']:
            if col in df_exploded.columns:
                df_exploded[col] = df_exploded[col].astype(str).str.replace(',', '')
                df_exploded[col] = pd.to_numeric(df_exploded[col], errors='coerce').round(2)

        df_exploded.rename(columns={
            'clean_item': 'items',
            'type/channel': 'order_type',
            'product_amount': 'total_order_amount'
        }, inplace=True)

        df_exploded['category'] = df_exploded['category'].fillna('Uncategorized')
        df_exploded['sub_category'] = df_exploded['sub_category'].fillna('Uncategorized')
        df_exploded = df_exploded.iloc[:-1]

        before = len(df_exploded)
        df_exploded.drop_duplicates(subset=['order_id', 'items', 'payment_time'], inplace=True)
        after = len(df_exploded)
        print(f"🧹 Dropped {before - after} duplicate rows")

        print(f"✅ Final cleanup done — {len(df_exploded)} rows")
        print(f"   Final columns: {list(df_exploded.columns)}")
        return df_exploded
    except Exception as e:
        print(f"❌ Failed during final cleanup: {e}")
        raise


# -----------------------------------------
# QUARANTINE FILTER
# -----------------------------------------

def split_good_and_bad(df):
    """
    Splits a DataFrame into good and quarantine rows.
    Returns (good_df, quarantine_df)
    """
    try:
        bad_mask = (
            df['items'].isna() |
            df['items'].astype(str).eq('nan') |
            df['total_order_amount'].isna() |
            df['order_id'].isna() |
            df['quantity'].le(0) |
            df['sub_category'].eq('Uncategorized') |
            df['category'].eq('Uncategorized')
        )

        good_df = df[~bad_mask].copy()
        quarantine_df = df[bad_mask].copy()

        if len(quarantine_df) > 0:
            quarantine_df.loc[quarantine_df['items'].isna(), 'quarantine_reason'] = 'Null item name'
            quarantine_df.loc[quarantine_df['total_order_amount'].isna(), 'quarantine_reason'] = 'Null order amount'
            quarantine_df.loc[quarantine_df['order_id'].isna(), 'quarantine_reason'] = 'Null order ID'
            quarantine_df.loc[quarantine_df['quantity'].le(0), 'quarantine_reason'] = 'Invalid quantity'
            quarantine_df['quarantine_timestamp'] = datetime.now(
                pytz.timezone("Asia/Manila")).date()
            quarantine_df.loc[quarantine_df['sub_category'].eq('Uncategorized'), 'quarantine_reason'] = 'Uncategorized sub_category'
            quarantine_df.loc[quarantine_df['category'].eq('Uncategorized'), 'quarantine_reason'] = 'Uncategorized category'

        print(f"✅ Split done — Good: {len(good_df)}, Quarantine: {len(quarantine_df)}")
        return good_df, quarantine_df
    except Exception as e:
        print(f"❌ Failed to split good and bad rows: {e}")
        raise


# -----------------------------------------
# GCS
# -----------------------------------------

def upload_parquet_to_gcs(local_path, destination_blob):
    """Uploads a local parquet file to GCS."""
    try:
        bucket_name = os.environ["GCS_BUCKET_NAME"]
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_path)
        print(f"✅ Uploaded to gs://{bucket_name}/{destination_blob}")
    except Exception as e:
        print(f"❌ Failed to upload to GCS: {e}")
        raise


def upload_parquet_to_gcs_amantes(local_path, destination_blob):
    """Uploads a local parquet file to GCS Amantes bucket."""
    try:
        bucket_name = os.environ["AMANTES_GCS_BUCKET"]
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_path)
        print(f"✅ Uploaded to gs://{bucket_name}/{destination_blob}")
    except Exception as e:
        print(f"❌ Failed to upload to GCS: {e}")
        raise


def archive_file(source_blob_name, timestamp):
    """Moves a file from /incoming to /processed/{timestamp}/."""
    try:
        bucket_name = os.environ["GCS_BUCKET_NAME"]
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        source = bucket.blob(source_blob_name)
        destination = f"processed/{timestamp}/cleaned_pos.parquet"
        bucket.copy_blob(source, bucket, destination)
        source.delete()
        print(f"✅ Archived to /processed/{timestamp}/")
    except Exception as e:
        print(f"❌ Failed to archive file: {e}")
        raise


def archive_files_new(source_path, namebucket, time):
    try:
        # Setup
        bucket_name = namebucket
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        source = bucket.blob(source_path)
        
        timestamp = f"{time}"
        filename = source_path.split("/")[-1]
        destination_path = f"processed/{timestamp}/{filename}"
        
        # 1. Execute the copy command
        bucket.copy_blob(source, bucket, destination_path)

        # 2. VERIFY WITH GCP: Ping the destination to ensure it physically exists
        destination_blob = bucket.blob(destination_path)
        if destination_blob.exists():
            source.delete()
            print(f"✅ Successfully archived: {filename}")
        else:
            raise FileNotFoundError(f"Verification failed: {filename} not found in GCS.")

    except Exception as e:
        print(f"⚠️ Critical error archiving {filename}. Error: {e}")
        raise e


# -----------------------------------------
# BIGQUERY
# -----------------------------------------

def load_parquet_to_bigquery(uri, table_id):
    """Loads a Parquet file from GCS into a BigQuery table."""
    try:
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        table = client.get_table(table_id)
        print(f"✅ Loaded into BigQuery: {table_id}")
        print(f"   Total rows in table: {table.num_rows}")
    except Exception as e:
        print(f"❌ Failed to load to BigQuery: {e}")
        raise




def load_parquet_to_bigquery_amantes_etl(uri, table_id):
    """Loads a Parquet file from GCS into a BigQuery table."""
    try:
        client = bigquery.Client()

        schema_staging_fact_sales = [
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("items", "STRING"),
            bigquery.SchemaField("sub_category", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("flavor", "STRING"),
            bigquery.SchemaField("variation", "STRING"),
            bigquery.SchemaField("size", "STRING"),
            bigquery.SchemaField("quantity", "FLOAT"),
            bigquery.SchemaField("spice_level", "STRING"),
            bigquery.SchemaField("sugar_level", "STRING"),
            bigquery.SchemaField("total_order_amount", "FLOAT"),
            bigquery.SchemaField("received_amount", "FLOAT"),
            bigquery.SchemaField("payment_time", "STRING"),
            bigquery.SchemaField("payment_type", "STRING"),
            bigquery.SchemaField("order_type", "STRING"),
            bigquery.SchemaField("source_file", "STRING"),
        ]
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema_staging_fact_sales, autodetect=False,
        )
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        table = client.get_table(table_id)
        print(f"✅ Loaded into BigQuery: {table_id}")
        print(f"   Total rows in table: {table.num_rows}")
    except Exception as e:
        print(f"❌ Failed to load to BigQuery: {e}")
        raise





def load_parquet_to_bigquery_quarantine(uri, table_id):
    try:
        client = bigquery.Client()

        schema_quarantine_fact_sales = [
                bigquery.SchemaField("order_id", "STRING"),
                bigquery.SchemaField("items", "STRING"),
                bigquery.SchemaField("sub_category", "STRING"),
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("flavor", "STRING"),
                bigquery.SchemaField("variation", "STRING"),
                bigquery.SchemaField("size", "STRING"),
                bigquery.SchemaField("quantity", "FLOAT"),
                bigquery.SchemaField("spice_level", "STRING"),
                bigquery.SchemaField("sugar_level", "STRING"),
                bigquery.SchemaField("total_order_amount", "FLOAT"),
                bigquery.SchemaField("received_amount", "FLOAT"),
                bigquery.SchemaField("payment_time", "STRING"),
                bigquery.SchemaField("payment_type", "STRING"),
                bigquery.SchemaField("order_type", "STRING"),
                bigquery.SchemaField("source_file", "STRING"),
                bigquery.SchemaField("quarantine_reason", "STRING"),
                bigquery.SchemaField("quarantine_timestamp", "DATE")
            ]

        job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema_quarantine_fact_sales, autodetect=False,
        )

        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        table = client.get_table(table_id)
        print(f"✅ Loaded into BigQuery: {table_id}")
        print(f"   Total rows in table: {table.num_rows}")

    except Exception as e:
        print(f"❌ Failed to load to BigQuery: {e}")
        raise
