# Import necessary modules
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import datetime

# Define a custom DoFn to parse CSV rows and enrich with metadata
class ParseEmployeeCSV(beam.DoFn):
    def process(self, element, timestamp, load_date):
        # Split the CSV line into individual fields
        row = element.split(',')

        # Yield a dictionary matching the BigQuery schema
        yield {
            'BusinessEntityID': str(row[0]),
            'NationalIDNumber': str(row[1]),
            'LoginID': str(row[2]),
            'DepartmentID': str(row[3]),
            'JobTitle': str(row[4]),
            'BirthDate': str(row[5]),
            'Gender': str(row[6]),
            'HireDate': str(row[7]),
            'SalariedFlag': str(row[8]),
            'VacationHours': str(row[9]),
            'SickLeaveHours': str(row[10]),
            'CurrentFlag': str(row[11]),
            'ModifiedDate': str(row[12]),
            'RawIngestionTime': timestamp,  # ISO 8601 timestamp for audit
            'LoadDate': load_date           # YYYY-MM-DD format for partitioning
        }

# Main pipeline function
def run():
    # Define pipeline options for Dataflow execution
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",  # Use Google Cloud Dataflow
        project="gcp-de-batch-sim-464816-476514",  # Your GCP project ID
        temp_location="gs://gcp-de-batch-data-4/temp",  # Temp bucket for staging files
        staging_location="gs://gcp-de-batch-data-4/staging",  # Staging bucket for pipeline artifacts
        region="us-east1",  # Regional endpoint for Dataflow
        job_name="employee-csv-ingestion"  # Unique job name
    )

    # Set the service account for the Dataflow job
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.service_account_email = "batch-sim@gcp-de-batch-sim-464816-476514.iam.gserviceaccount.com"

    # Set pipeline to batch mode
    pipeline_options.view_as(StandardOptions).streaming = False

    # Define input file path and metadata
    gcs_file_path = "gs://gcp-de-batch-data-4/Employee.csv"  # GCS path to input CSV
    ingestion_time = datetime.datetime.utcnow().isoformat()  # Current UTC timestamp
    load_date = datetime.datetime.utcnow().date().isoformat()  # Current date in YYYY-MM-DD format

    # Build and run the Beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            # Read CSV lines from GCS, skipping the header row
            | "Read CSV" >> beam.io.ReadFromText(gcs_file_path, skip_header_lines=1)

            # Parse each line and enrich with ingestion metadata
            | "Parse and Enrich" >> beam.ParDo(ParseEmployeeCSV(), timestamp=ingestion_time, load_date=load_date)

            # Write enriched records to BigQuery
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table="gcp-de-batch-sim-464816-476514.Employee_Details_raw.Employee_raw",  # Full table path
                schema={
                    'fields': [
                        {'name': 'BusinessEntityID', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'NationalIDNumber', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'LoginID', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'DepartmentID', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'JobTitle', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'BirthDate', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'Gender', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'HireDate', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'SalariedFlag', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'VacationHours', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'SickLeaveHours', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'CurrentFlag', 'type': 'STRING', 'mode': 'REQUIRED'},
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