import argparse  # Import module to parse command line arguments
import json  # Import module to work with JSON data
import apache_beam as beam  # Import Apache Beam for building data pipelines
from apache_beam.options.pipeline_options import PipelineOptions  # Import PipelineOptions for pipeline config
import csv  # Import csv module to parse CSV lines


def load_json(path):
    # Open a JSON file at given path and load its content into a Python dictionary
    with open(path) as f:
        return json.load(f)


def schema_to_bq_string(schema_list):
    # Convert list of BigQuery schema field dicts to a string "name:type,name:type,..." format used by BQ
    return ",".join(f"{field.get('name', '')}:{field.get('type', '')}" for field in schema_list)


def extract_field_properties(schema_list):
    # Extract lists of fields names, required fields, nullable fields from BigQuery schema list
    field_names = []
    required_fields = set()
    nullable_fields = set()
    for field in schema_list:
        name = field.get('name')  # Get field name
        mode = field.get('mode', 'NULLABLE').upper()  # Get mode (REQUIRED or NULLABLE), default to NULLABLE
        field_names.append(name)  # Collect all field names
        if mode == 'REQUIRED':
            required_fields.add(name)  # Track required fields
        elif mode == 'NULLABLE':
            nullable_fields.add(name)  # Track nullable fields
    return field_names, required_fields, nullable_fields


def parse_csv_line(line, field_names):
    # Parse a single CSV line into a dictionary mapping field names to values
    values = next(csv.reader([line]))  # Parse CSV line into a list of values
    # Extend values with None for missing trailing values if fewer columns than expected
    if len(values) < len(field_names):
        values.extend([None] * (len(field_names) - len(values)))
    # Return dictionary of field_name: value pairs zipped together
    return dict(zip(field_names, values))


def clean_row(row, nullable_fields):
    # Clean a row by replacing "NULL" or empty strings with None for nullable fields only
    if row is None:
        return None  # If no row, return None directly
    # Replace values in nullable fields that are "NULL" or empty string with None, else keep original value
    return {k: (None if (v in ("NULL", "")) and k in nullable_fields else v) for k, v in row.items()}


def validate_row(row, required_fields, field_names, table_name):
    # Validate a row by checking no missing required fields and no unexpected field names
    if row is None:
        print(f"{table_name}: Dropping None row")
        return False  # Drop row if None
    if set(row.keys()) != set(field_names):
        print(f"{table_name}: Dropping row with unexpected fields: {row}")
        return False  # Drop if fields don't match schema exactly
    # Drop if any required field is missing or empty
    for field in required_fields:
        val = row.get(field)
        if val is None or val == '':
            print(f"{table_name}: Dropping row missing required '{field}': {row}")
            return False
    return True  # Row is valid


def run(project_id, dataset_id, table_name, input_path, config_path, schema_path, options_path):
    # Main function to run Apache Beam pipeline for the specified BigQuery table
    print(f"Running pipeline with table_name={table_name}, input_path={input_path}")

    # Load config, schema, and pipeline options JSON files as dictionaries
    config = load_json(config_path)
    schemas = load_json(schema_path)
    options_dict = load_json(options_path)

    # Create Beam pipeline options object from options dict
    pipeline_options = PipelineOptions(**options_dict)

    # Get schema for the specific table from loaded schemas
    schema = schemas[table_name]
    
    # Extract field names and sets of required and nullable fields
    field_names, required_fields, nullable_fields = extract_field_properties(schema)

    # Convert schema to BigQuery schema string format "name:type, ..."
    bq_schema_str = schema_to_bq_string(schema)

    # Create BigQuery table spec string "project:dataset.table"
    table_spec = f"{project_id}:{dataset_id}.{table_name}"

    # Define a Beam metrics counter to track number of rows processed for this table
    rows_processed_counter = beam.metrics.Metrics.counter('pipeline', f'{table_name}_rows_processed')

    # Function to increment row processed counter for monitoring
    def count_row(row):
        rows_processed_counter.inc()
        return row

    # Define the Apache Beam pipeline using context manager so it runs automatically
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            # Read lines from input CSV file, skip header line
            | f"Read_{table_name}" >> beam.io.ReadFromText(input_path, skip_header_lines=1)
            # Parse each CSV line into dict of field_name: value
            | f"Parse_{table_name}" >> beam.Map(lambda r: parse_csv_line(r, field_names))
            # Clean each row, converting "NULL" or "" in nullable fields to None
            | f"Clean_{table_name}" >> beam.Map(lambda r: clean_row(r, nullable_fields))
            # Filter out any None rows after cleaning
            | f"FilterNone_{table_name}" >> beam.Filter(lambda r: r is not None)
            # Validate rows; only keep those that pass validation
            | f"Validate_{table_name}" >> beam.Filter(lambda r: validate_row(r, required_fields, field_names, table_name))
            # Count rows passing validation for metrics
            | f"CountRows_{table_name}" >> beam.Map(count_row)
            # Write resulting rows to BigQuery table with specified schema and dispositions
            | f"WriteToBigQuery_{table_name}" >> beam.io.WriteToBigQuery(
                    table_spec,
                    schema=bq_schema_str,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    custom_gcs_temp_location=options_dict.get("temp_location"),
                )
        )


# Entry point of the script when run from command line
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Dataflow pipeline for a specific table")
    # Define required command line arguments for GCP project details and paths to configs and input data
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--dataset_id", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--config_path", default="../config/table_config.json")  # Optional config path with default
    parser.add_argument("--schema_path", default="../config/bq_schemas.json")  # Optional BQ schema path with default
    parser.add_argument("--options_path", default="../config/dataflow_options.json")  # Optional pipeline options path
    
    # Parse the passed command line arguments
    args = parser.parse_args()

    # Call main pipeline run function with parsed arguments
    run(
        args.project_id,
        args.dataset_id,
        args.table_name,
        args.input_path,
        args.config_path,
        args.schema_path,
        args.options_path,
    )
