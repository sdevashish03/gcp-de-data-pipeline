# 📦 Import required libraries
# Explanation: Standard imports for Apache Beam pipeline, BigQuery I/O, and datetime handling.
# Added GoogleCloudOptions and StandardOptions for full pipeline options (e.g., service account, streaming).
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime


# 🛠️ Static configuration
# Explanation: Project and resource configs from provided code.
# SERVICE_ACCOUNT: Added same-project SA (adjust as needed); comment usage below for default Compute Engine SA to avoid bindings.
project = "gcp-de-batch-sim-464816-476514"
region = "us-east1"
bucket = "gcp-de-batch-data-4"
raw_dataset = "Employee_Details_raw"
staging_dataset = "Employee_Details_stg"
raw_table = "Employee_raw"
staging_table = "Employee_stg"

input = f"{project}.{raw_dataset}.{raw_table}"
output = f"{project}.{staging_dataset}.{staging_table}"
temp_location = f"gs://{bucket}/temp"
staging_location = f"gs://{bucket}/staging"
SERVICE_ACCOUNT = "batch-sim@gcp-de-batch-sim-464816-476514.iam.gserviceaccount.com"  # Same-project SA; comment if using default


# 📐 BigQuery schema for staging table
# Explanation: Schema from provided code, reverted DepartmentID to 'STRING' to match existing table schema.
# This prevents mismatch errors during append to existing table.
# If table doesn't exist, CREATE_IF_NEEDED will use this schema.
# Other fields unchanged.
schema = {
    'fields': [
        {'name': 'BusinessEntityID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'NationalIDNumber', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'LoginID', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'DepartmentID', 'type': 'STRING', 'mode': 'REQUIRED'},  # STRING to match existing table; handled as int in code then str output
        {'name': 'JobTitle', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'BirthDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'Gender', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'HireDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'SalariedFlag', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
        {'name': 'VacationHours', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'SickLeaveHours', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'CurrentFlag', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
        {'name': 'ModifiedDate', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'RawIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'LoadDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'StagingIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ]
}


# 🔄 Robust date parsing helper
# Explanation: Enhanced from original parse_date; tries multiple formats for DATE fields (BirthDate, HireDate, LoadDate).
# Added fallbacks: If all fail, use a default date (e.g., '1900-01-01') or current; return None on critical errors.
# Supports '%Y-%m-%d', '%m/%d/%Y', and full timestamps (extract date).
def parse_date(date_str, is_required=True):
    if not date_str:
        return datetime(1900, 1, 1).date() if is_required else None  # Default if empty
    date_str = str(date_str).strip()
    formats = ['%Y-%m-%d', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']  # Common formats
    try:
        for fmt in formats:
            try:
                if len(date_str) > 10:  # Full timestamp, extract date
                    dt = datetime.strptime(date_str, fmt)
                    return dt.date()
                else:  # Date-only
                    dt = datetime.strptime(date_str, fmt)
                    return dt.date()
            except ValueError:
                continue
        # Fallback: Default date or raise if required
        default_date = datetime(1900, 1, 1).date()
        print(f"Warning: Defaulting date '{date_str}' to {default_date}")
        return default_date
    except Exception as e:
        print(f"Error parsing date '{date_str}': {e}")
        return None if not is_required else datetime.now().date()  # Skip or current as fallback


# 🔄 Robust timestamp parsing helper
# Explanation: New helper for TIMESTAMP fields (ModifiedDate, RawIngestionTime).
# Tries multiple formats; handles time-only/malformed like previous (e.g., '00:00.0' -> prepend default date).
# Returns ISO string with 'Z' for BigQuery; defaults on failure.
def parse_timestamp(ts_str, is_required=True):
    if not ts_str:
        return '1900-01-01T00:00:00Z' if is_required else None
    ts_str = str(ts_str).strip()
    try:
        if 'Z' in ts_str or 'T' in ts_str:  # ISO format
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return dt.isoformat() + 'Z'
        elif len(ts_str) > 10:  # Full datetime, e.g., '%Y-%m-%d %H:%M:%S'
            dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
            return dt.isoformat() + 'Z'
        elif ':' in ts_str and len(ts_str) < 10:  # Time-only, e.g., '00:00.0'
            try:
                default_date = '1900-01-01 '
                full_str = default_date + ts_str
                if '.' in ts_str:  # Fractional, '%H:%M.%f'
                    dt = datetime.strptime(full_str, '%Y-%m-%d %H:%M.%f')
                else:  # '%H:%M:%S'
                    dt = datetime.strptime(full_str, '%Y-%m-%d %H:%M:%S')
                return dt.isoformat() + 'Z'
            except ValueError:
                pass
        # Final fallback
        default_ts = datetime(1900, 1, 1)
        print(f"Warning: Defaulting timestamp '{ts_str}' to {default_ts}")
        return default_ts.isoformat() + 'Z'
    except Exception as e:
        print(f"Error parsing timestamp '{ts_str}': {e}")
        return datetime.utcnow().isoformat() + 'Z' if is_required else None


# 🔄 Convert field types to match schema
# Explanation: Enhanced from original convert_types; now robust with try-except, multiple format parsing, and defaults.
# Handles type conversions safely (e.g., int() with fallback, boolean from strings).
# For DepartmentID: Parse as int (validate numeric) then convert to str() for STRING schema/output.
# This "handles as integer in code" (e.g., can add int logic if needed) but writes as string to match existing table.
# If critical failure (e.g., missing required field), return None to filter out bad records.
def convert_types(record):
    try:
        # Basic string conversions (safe str)
        national_id = str(record.get('NationalIDNumber', ''))
        login_id = str(record.get('LoginID', ''))
        job_title = str(record.get('JobTitle', ''))
        gender = str(record.get('Gender', ''))

        # Integer conversions with fallback (0 on failure)
        business_entity_id = int(record.get('BusinessEntityID', 0))
        department_id_raw = record.get('DepartmentID', '0')
        # Handle DepartmentID as integer in code: Parse int, then str for STRING output
        try:
            department_id_int = int(department_id_raw) if department_id_raw else 0
            department_id = str(department_id_int)  # Convert back to str for schema
        except (ValueError, TypeError):
            department_id = str(department_id_raw)  # Fallback to raw if not numeric
            print(f"Warning: Non-numeric DepartmentID '{department_id_raw}' treated as string")
        vacation_hours = int(record.get('VacationHours', 0))
        sick_leave_hours = int(record.get('SickLeaveHours', 0))

        # Boolean conversions (handle string variants)
        salaried_flag_str = str(record.get('SalariedFlag', 'false')).lower()
        salaried_flag = salaried_flag_str in ['true', '1', 't', 'yes']
        current_flag_str = str(record.get('CurrentFlag', 'false')).lower()
        current_flag = current_flag_str in ['true', '1', 't', 'yes']

        # Date and Timestamp parsing
        birth_date = parse_date(record.get('BirthDate'))
        hire_date = parse_date(record.get('HireDate'))
        modified_date = parse_timestamp(record.get('ModifiedDate'))
        raw_ingestion_time = parse_timestamp(record.get('RawIngestionTime'))
        load_date = parse_date(record.get('LoadDate'))

        # Staging ingestion (UTC for TIMESTAMP)
        staging_ingestion_time = datetime.utcnow().isoformat() + 'Z'

        # Check for critical nulls (e.g., if parsing returned None for required fields)
        if None in [birth_date, hire_date, modified_date, raw_ingestion_time, load_date]:
            print(f"Warning: Skipping record due to unparsable date/timestamp: {record}")
            return None

        return {
            'BusinessEntityID': business_entity_id,
            'NationalIDNumber': national_id,
            'LoginID': login_id,
            'DepartmentID': department_id,  # String representation of integer value
            'JobTitle': job_title,
            'BirthDate': birth_date.strftime('%Y-%m-%d'),  # DATE string for BigQuery
            'Gender': gender,
            'HireDate': hire_date.strftime('%Y-%m-%d'),
            'SalariedFlag': salaried_flag,
            'VacationHours': vacation_hours,
            'SickLeaveHours': sick_leave_hours,
            'CurrentFlag': current_flag,
            'ModifiedDate': modified_date,
            'RawIngestionTime': raw_ingestion_time,
            'LoadDate': load_date.strftime('%Y-%m-%d') if load_date else datetime.now().strftime('%Y-%m-%d'),
            'StagingIngestionTime': staging_ingestion_time
        }
    except (ValueError, KeyError, TypeError) as e:
        print(f"Transformation error for record {record}: {e}")
        return None  # Skip bad records


# Filter out None records to avoid writing invalid data
# Explanation: New filter; skips records where convert_types returned None (e.g., parsing failures or missing fields).
def filter_valid_records(record):
    return record is not None


# 🚀 Define and run the pipeline
# Explanation: Pipeline setup from original.
# Revisions: Use enhanced convert_types (includes StagingIngestionTime); add filter.
# Service account commented for default (no bindings); add experiments for performance.
def run():
    # ⚙️ Set pipeline options
    # Explanation: DataflowRunner for batch; added experiments.
    options = PipelineOptions(
        runner='DataflowRunner',
        project=project,
        region=region,
        temp_location=temp_location,
        staging_location=staging_location,
        job_name='emp-staging-job',
        save_main_session=True,
        experiments=['use_runner_v2']  # Optional for improved execution (Beam >=2.40)
    )

    # 🔐 Service account setup (comment for default Compute Engine SA)
    # Explanation: Defaults to project-compute@developer.gserviceaccount.com if commented.
    # Ensure default SA has roles: dataflow.worker, bigquery.dataEditor, storage.objectAdmin.
    google_options = options.view_as(GoogleCloudOptions)
    # google_options.service_account_email = SERVICE_ACCOUNT  # Uncomment for custom SA
    options.view_as(StandardOptions).streaming = False

    # 🛠️ Build the pipeline
    # Explanation: Read from raw; convert/enrich; filter valid; write with append.
    # Removed separate add_StagingIngestionTime (now in convert_types).
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(
                query=f'SELECT * FROM `{input}`', 
                use_standard_sql=True
            )
            | 'Convert Field Types' >> beam.Map(convert_types)
            | 'Filter Valid Records' >> beam.Filter(filter_valid_records)  # Skip invalid
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=output,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )


# 🏁 Entry point
# Explanation: No changes; runs the pipeline when executed.
if __name__ == '__main__':
    run()