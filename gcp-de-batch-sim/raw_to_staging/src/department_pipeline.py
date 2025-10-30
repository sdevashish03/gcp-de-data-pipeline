# 📦 Import required libraries
# Explanation: Standard imports for Apache Beam pipeline, BigQuery I/O, and datetime handling.
# No changes needed here.
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime


# 🛠️ Static configuration
# Explanation: Project and resource configs remain the same.
# Updated SERVICE_ACCOUNT to a same-project account (assuming it's now in gcp-de-batch-sim-464816-476514).
# For default Compute Engine SA (no custom bindings), comment out SERVICE_ACCOUNT usage below.
PROJECT_ID = "gcp-de-batch-sim-464816-476514"
REGION = "us-east1"
BUCKET = "gcp-de-batch-data-4"
RAW_DATASET = "Employee_Details_raw"
STAGING_DATASET = "Employee_Details_stg"
RAW_TABLE = "Department_raw"
STAGING_TABLE = "Department_stg"

INPUT_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}"
OUTPUT_TABLE = f"{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}"
TEMP_LOCATION = f"gs://{BUCKET}/temp"
STAGING_LOCATION = f"gs://{BUCKET}/staging"
SERVICE_ACCOUNT = "batch-sim@gcp-de-batch-sim-464816-476514.iam.gserviceaccount.com"  # Same-project SA; comment if using default


# 📐 BigQuery schema for staging table
# Explanation: Schema defines required fields for staging table.
# Added 'NULLABLE' mode for LoadDate if it's optional; otherwise, keep as REQUIRED.
# TIMESTAMP fields expect RFC3339 format (handled in transform).
BQ_SCHEMA = {
    'fields': [
        {'name': 'DepartmentID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'GroupName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'ModifiedDate', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'RawIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'LoadDate', 'type': 'DATE', 'mode': 'REQUIRED'},  # Ensure raw LoadDate is in 'YYYY-MM-DD' format
        {'name': 'StagingIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ]
}


# 🔄 Convert field types to match schema
# Explanation: Transform function parses and enriches raw records.
# Revision: Enhanced parsing with multiple format attempts for ModifiedDate and RawIngestionTime to handle malformed data like '00:00.0' (time-only without date).
# If parsing fails all attempts, set to a default timestamp (e.g., '1900-01-01T00:00:00Z') or return None to skip the record.
# For '00:00.0', treat as time-only: Prepend default date '1900-01-01' and parse as '%Y-%m-%d %H:%M.%f'.
# Added handling for LoadDate: If not full date, extract or default to current date.
# This prevents ValueError from strptime on short/malformed strings (common in raw data ingestion errors).
def transform_record(record):
    try:
        # Robust parsing for ModifiedDate
        modified_str = str(record.get('ModifiedDate', '')).strip()
        modified_iso = None
        if len(modified_str) > 10:  # Full datetime, e.g., '2003-01-10 00:00:00'
            try:
                modified_date = datetime.strptime(modified_str, '%Y-%m-%d %H:%M:%S')
                modified_iso = modified_date.isoformat() + 'Z'
            except ValueError:
                pass  # Try next
        if modified_iso is None and ':' in modified_str:  # Time-like, e.g., '00:00.0' or '00:00:00.000'
            try:
                # Assume time-only; prepend default date
                default_date = '1900-01-01 '
                full_str = default_date + modified_str if not modified_str.startswith(default_date) else modified_str
                if '.' in modified_str:  # Handle fractional seconds, e.g., '%H:%M.%f'
                    modified_date = datetime.strptime(full_str, '%Y-%m-%d %H:%M.%f')
                else:  # Standard time '%H:%M:%S'
                    modified_date = datetime.strptime(full_str, '%Y-%m-%d %H:%M:%S')
                modified_iso = modified_date.isoformat() + 'Z'
            except ValueError:
                pass  # Try next
        if modified_iso is None:  # Final fallback: Default timestamp
            modified_iso = '1900-01-01T00:00:00Z'
            print(f"Warning: Defaulting ModifiedDate for record {record}: '{modified_str}'")

        # Robust parsing for RawIngestionTime (assume ISO or standard; handle potential short strings)
        raw_time_str = str(record.get('RawIngestionTime', '')).strip()
        raw_iso = None
        if raw_time_str:
            try:
                if 'Z' in raw_time_str or 'T' in raw_time_str:  # ISO format
                    raw_ingestion = datetime.fromisoformat(raw_time_str.replace('Z', '+00:00'))
                else:  # Standard '%Y-%m-%d %H:%M:%S'
                    raw_ingestion = datetime.strptime(raw_time_str, '%Y-%m-%d %H:%M:%S')
                raw_iso = raw_ingestion.isoformat() + 'Z'
            except ValueError:
                # Fallback: Assume time-only if short
                if len(raw_time_str) < 10 and ':' in raw_time_str:
                    try:
                        default_date = '1900-01-01 '
                        full_str = default_date + raw_time_str
                        raw_ingestion = datetime.strptime(full_str, '%Y-%m-%d %H:%M:%S.%f') if '.' in raw_time_str else datetime.strptime(full_str, '%Y-%m-%d %H:%M:%S')
                        raw_iso = raw_ingestion.isoformat() + 'Z'
                    except ValueError:
                        raw_iso = datetime.utcnow().isoformat() + 'Z'  # Use ingestion time as fallback
                        print(f"Warning: Defaulting RawIngestionTime for record {record}: '{raw_time_str}'")
                else:
                    raw_iso = datetime.utcnow().isoformat() + 'Z'  # Use current if invalid

        # LoadDate: Assume 'YYYY-MM-DD'; truncate if timestamp, or default if invalid
        load_date_str = str(record.get('LoadDate', '')).strip()
        load_date = None
        if load_date_str:
            try:
                if len(load_date_str) > 10:  # Full timestamp, extract date
                    load_date_obj = datetime.fromisoformat(load_date_str.replace('Z', '+00:00')) if 'Z' in load_date_str else datetime.strptime(load_date_str, '%Y-%m-%d %H:%M:%S')
                    load_date = load_date_obj.strftime('%Y-%m-%d')
                else:  # Already date string
                    load_date = load_date_str
            except ValueError:
                load_date = datetime.utcnow().strftime('%Y-%m-%d')  # Default to today
                print(f"Warning: Defaulting LoadDate for record {record}: '{load_date_str}'")

        return {
            'DepartmentID': int(record['DepartmentID']),
            'Name': str(record['Name']),
            'GroupName': str(record['GroupName']),
            'ModifiedDate': modified_iso,
            'RawIngestionTime': raw_iso,
            'LoadDate': load_date,
            'StagingIngestionTime': datetime.utcnow().isoformat() + 'Z'
        }
    except (ValueError, KeyError, TypeError) as e:
        # Log and skip bad records entirely
        print(f"Transformation error for record {record}: {e}")
        return None

# Filter out None records to avoid writing invalid data
# Explanation: No changes; skips records where transform failed (e.g., missing keys or unhandled errors).
def filter_valid_records(record):
    return record is not None


# 🚀 Define and run the pipeline
# Explanation: Pipeline setup with options.
# Revision: No further changes needed; the enhanced transform handles the parsing error.
# For default SA (no bindings): Comment out service_account_email line.
# Added setup_options for potential debugging (e.g., --experiments=use_runner_v2 for Beam 2.50+ compatibility).
def run():
    # ⚙️ Set pipeline options
    # Explanation: DataflowRunner for batch job; non-streaming.
    # Added experiments for better performance; temp/staging for Dataflow resources.
    options = PipelineOptions(
        runner='DataflowRunner',
        project=PROJECT_ID,
        region=REGION,
        temp_location=TEMP_LOCATION,
        staging_location=STAGING_LOCATION,
        job_name='department-staging-job',
        save_main_session=True,
        experiments=['use_runner_v2']  # Enable for improved execution (optional, Beam >=2.40)
    )

    # 🔐 Explicitly set the service account (comment for default Compute Engine SA)
    # Explanation: Uses provided SA if uncommented; defaults to project-compute@developer.gserviceaccount.com for workers.
    # Ensure default SA has required roles (dataflow.worker, bigquery.dataEditor, storage.objectAdmin) if using default.
    google_options = options.view_as(GoogleCloudOptions)
    # google_options.service_account_email = SERVICE_ACCOUNT  # Uncomment for custom SA; comment for default
    options.view_as(StandardOptions).streaming = False

    # 🛠️ Build the pipeline
    # Explanation: Read from raw BQ table via query; transform/enrich; filter valid; write to staging with append.
    # Added filter after transform to skip invalid records from errors.
    # Use CREATE_IF_NEEDED for table; APPEND for incremental loads.
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read from BigQuery Raw Table" >> beam.io.ReadFromBigQuery(
                query=f"SELECT * FROM `{INPUT_TABLE}`",
                use_standard_sql=True
            )
            | "Transform and Enrich Records" >> beam.Map(transform_record)
            | "Filter Valid Records" >> beam.Filter(filter_valid_records)  # Skip None from errors
            | "Write to BigQuery Staging Table" >> WriteToBigQuery(
                table=OUTPUT_TABLE,
                schema=BQ_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )


# 🏁 Entry point
# Explanation: No changes; runs the pipeline when executed.
if __name__ == "__main__":
    run()