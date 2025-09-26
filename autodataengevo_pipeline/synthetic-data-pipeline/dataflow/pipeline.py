# Import Apache Beam core module
import apache_beam as beam

# Import PipelineOptions to configure runtime parameters
from apache_beam.options.pipeline_options import PipelineOptions

# Import csv module to parse CSV lines
import csv

# Import FileSystems to read header line from GCS
from apache_beam.io.filesystems import FileSystems

# Import datetime to generate ingestion timestamp
from datetime import datetime


# ✅ Define custom pipeline options to accept CLI arguments
class RawLoadOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--service_account_email', required=False)
        parser.add_argument('--input_path', required=True)         # GCS path to input CSV file
        parser.add_argument('--output_table', required=True)       # BigQuery table (project:dataset.table)
        parser.add_argument('--project', required=True)            # GCP project ID
        parser.add_argument('--region', default='us-central1')     # GCP region
        parser.add_argument('--temp_location', required=True)      # GCS bucket for staging
        parser.add_argument('--runner', default='DataflowRunner')  # Runner type


# ✅ Function to parse a CSV line into a dictionary and inject audit column
def parse_csv_with_audit(line, header):
    values = next(csv.reader([line]))               # Convert line into list of values
    row = dict(zip(header, values))                 # Zip header with values to form a dictionary
    row['RawIngestionTime'] = datetime.utcnow().isoformat()  # Add current UTC timestamp
    return row


# ✅ Main pipeline logic
def run():
    options = RawLoadOptions()                      # Parse CLI options
    p = beam.Pipeline(options=options)              # Create Beam pipeline

    # Read header line from GCS to extract column names
    with FileSystems.open(options.input_path) as f:
        header = next(f).decode('utf-8').strip().split(',')

    # Define pipeline steps
    (
        p
        | 'Read CSV' >> beam.io.ReadFromText(options.input_path, skip_header_lines=1)  # Skip header
        | 'Parse Rows with Audit' >> beam.Map(lambda line: parse_csv_with_audit(line, header))  # Convert to dict + timestamp
        | 'Write to BQ' >> beam.io.WriteToBigQuery(
            options.output_table,                                                      # Target table
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,               # Append mode
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,          # Create if missing
            custom_gcs_temp_location=options.temp_location                             # Temp bucket
        )
    )

    p.run().wait_until_finish()              # Run pipeline and wait for completion


# ✅ Entry point
if __name__ == '__main__':
    run()