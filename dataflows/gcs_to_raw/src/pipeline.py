import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import datetime
from io import StringIO
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
 
class ParseCSV(beam.DoFn):
    """Parses CSV records and adds metadata fields."""
    def process(self, element, timestamp, file_size):
        row = element.split(',')
        yield {
            'DepartmentID': str(row[0]),
            'Name': str(row[1]),
            'GroupName': str(row[2]),
            'ModifiedDate': str(row[3]),
            'RawIngestionTime': str(timestamp),  # Capture ingestion timestamp
            'RawFileSize': int(file_size)  # Capture file size
        }
 
def get_file_size(gcs_file_path, pipeline_options):
    """Gets the file size of the CSV from GCS."""
    gcs = GCSFileSystem(pipeline_options)
    matched_files = gcs.match([gcs_file_path])
   
    if matched_files and matched_files[0].metadata_list:
        return matched_files[0].metadata_list[0].size_in_bytes
    return None
 
def run():
    # Define pipeline options
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project="gcp-de-batch-sim",
        temp_location="gs://gcp-de-batch-data-2/temp",
        staging_location="gs://gcp-de-batch-data-2/staging",
        region="us-east1"  
    )
 
    gcs_file_path = "gs://gcp-de-batch-data-2/Department.csv"
 
    # Capture ingestion time
    ingestion_time = str(datetime.datetime.now())
 
    # Get file size from GCS
    file_size = get_file_size(gcs_file_path, pipeline_options)
 
    with beam.Pipeline(options=pipeline_options) as p:
        raw_data = (
            p
            | "Read from GCS" >> beam.io.ReadFromText(gcs_file_path, skip_header_lines=1)
            | "CSV Parsing" >> beam.ParDo(ParseCSV(), timestamp=ingestion_time, file_size=file_size)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table="gcp-de-batch-sim.Employee_Details_raw.Department_raw",
                schema = {
                    'fields': [
                        {'name': 'DepartmentID', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'Name', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'GroupName', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'ModifiedDate', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'RawIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
                        {'name': 'RawFileSize', 'type': 'INTEGER', 'mode': 'REQUIRED'}
                    ]
                }
            )
        )
 
# Run the pipeline
if __name__ == '__main__':
    run()