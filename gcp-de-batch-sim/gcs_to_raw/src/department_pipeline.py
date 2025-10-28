# Import Apache Beam and required modules
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import datetime

# Define a custom DoFn to parse CSV rows and add metadata fields
class ParseCSV(beam.DoFn):
    """Parses CSV records and adds ingestion metadata."""
    def process(self, element, timestamp, load_date):
        # Split the CSV line into fields
        row = element.split(',')
        # Yield a dictionary matching the BigQuery schema
        yield {
            'DepartmentID': str(row[0]),
            'Name': str(row[1]),
            'GroupName': str(row[2]),
            'ModifiedDate': str(row[3]),
            'RawIngestionTime': timestamp,  # ISO timestamp for ingestion
            'LoadDate': load_date           # YYYY-MM-DD format for partitioning
        }

# Main pipeline function
def run():
    # Define pipeline options for Dataflow
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",  # Run on Google Cloud Dataflow
        project="gcp-de-batch-sim-464816-476514",  # Your actual GCP project ID
        temp_location="gs://gcp-de-batch-data-4/temp",  # Temp bucket for staging
        staging_location="gs://gcp-de-batch-data-4/staging",  # Staging bucket
        region="us-east1",  # Dataflow region
        job_name="department-csv-ingestion"  # Unique job name
    )

    # Explicitly set the service account to run the Dataflow job
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.service_account_email = "batch-sim@gcp-de-batch-sim-464816-476514.iam.gserviceaccount.com"

    # Disable streaming mode (this is a batch job)
    pipeline_options.view_as(StandardOptions).streaming = False

    # GCS path to the input CSV file
    gcs_file_path = "gs://gcp-de-batch-data-4/Department.csv"

    # Capture ingestion timestamp in ISO format (for TIMESTAMP field)
    ingestion_time = datetime.datetime.utcnow().isoformat()

    # Capture load date in YYYY-MM-DD format (for DATE field)
    load_date = datetime.datetime.utcnow().date().isoformat()

    # Build and run the Beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            # Read CSV lines from GCS, skipping the header
            | "Read from GCS" >> beam.io.ReadFromText(gcs_file_path, skip_header_lines=1)

            # Parse each line and enrich with metadata
            | "CSV Parsing" >> beam.ParDo(ParseCSV(), timestamp=ingestion_time, load_date=load_date)

            # Write enriched records to BigQuery
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table="gcp-de-batch-sim-464816-476514.Employee_Details_raw.Department_raw",  # Full table path
                schema={
                    'fields': [
                        {'name': 'DepartmentID', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'Name', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'GroupName', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'ModifiedDate', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'RawIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
                        {'name': 'LoadDate', 'type': 'DATE', 'mode': 'REQUIRED'}
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,  # Append to existing table
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED  # Create table if not exists
            )
        )

# Entry point for script execution
if __name__ == '__main__':
    run()