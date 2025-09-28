import json  # For reading JSON config/schema files
import apache_beam as beam  # Apache Beam SDK for pipeline coding
from apache_beam.options.pipeline_options import PipelineOptions  # For Dataflow/Beam options
import csv  # For accurate CSV parsing
from apache_beam.metrics import Metrics  # To track rows processed metrics


def load_json(path):
    # Read JSON file contents and parse into dict
    with open(path) as f:
        return json.load(f)


def schema_to_bq_string(schema_list):
    # Transform BigQuery schema list of dicts into Beam BQ schema string
    return ",".join(f"{field.get('name', '')}:{field.get('type', '')}" for field in schema_list)


def extract_fields(schema_list):
    # Dynamically extract field names, required fields, and nullable fields from schema JSON
    field_names = []
    required_fields = set()
    nullable_fields = set()
    for field in schema_list:
        name = field.get('name')
        mode = field.get('mode', 'NULLABLE').upper()
        field_names.append(name)
        if mode == 'REQUIRED':
            required_fields.add(name)
        elif mode == 'NULLABLE':
            nullable_fields.add(name)
    return field_names, required_fields, nullable_fields


def parse_csv_row(row, field_names):
    # Parse CSV row string into dict with expected field names
    csv_fields = next(csv.reader([row]))
    if len(csv_fields) != len(field_names):
        print(f"Warning: skipping row with incorrect number of columns: {row}")
        return None
    return dict(zip(field_names, csv_fields))


def clean_row(row, nullable_fields):
    # Convert "NULL" strings to None for only nullable fields, keep other fields as is
    if row is None:
        return None
    return {
        k: (None if (v in ("NULL", "")) and k in nullable_fields else v)
        for k, v in row.items()
    }


def validate_row(row, required_fields, field_names, table_name):
    # Check row contains all fields and all required fields are non-null/non-empty
    if row is None:
        print(f"{table_name}: Dropping None row")
        return False
    if len(row) != len(field_names):
        print(f"{table_name}: Dropping row with incorrect number of fields: {row}")
        return False
    for field in required_fields:
        val = row.get(field)
        if val is None or val == '':
            print(f"{table_name}: Dropping row missing required field '{field}': {row}")
            return False
    return True

def enforce_schema(row, field_names):
    # Ensures BigQuery rows match schema exactly
    return {field: row.get(field, None) for field in field_names}


def run_pipeline(config_path, schema_path, options_path):
    # Load config files for pipeline settings, BigQuery schemas, and Dataflow options
    config = load_json(config_path)
    schemas = load_json(schema_path)
    options_dict = load_json(options_path)
    options = PipelineOptions(**options_dict)

    with beam.Pipeline(options=options) as p:
        for table in config['tables']:
            table_name = table['name']
            gcs_path = table['source_path']

            schema = schemas[table_name]
            field_names, required_fields, nullable_fields = extract_fields(schema)

            table_spec = f"{options_dict['project']}:{config['dataset_id']}.{table_name}"
            bq_schema_str = schema_to_bq_string(schema)

            rows_processed_counter = Metrics.counter('pipeline', f'{table_name}_rows_processed')

            def count_row(row):
                rows_processed_counter.inc()
                return row

            (
                    p
                    | f"Read_{table_name}" >> beam.io.ReadFromText(gcs_path, skip_header_lines=1)
                    | f"Parse_{table_name}" >> beam.Map(lambda r: parse_csv_row(r, field_names))
                    | f"Clean_{table_name}" >> beam.Map(lambda r: clean_row(r, nullable_fields))
                    | f"Validate_{table_name}" >> beam.Filter(lambda r: validate_row(r, required_fields, field_names, table_name))
                    | f"FilterNull_{table_name}" >> beam.Filter(lambda r: r is not None)
                    | f"EnforceSchema_{table_name}" >> beam.Map(lambda r: enforce_schema(r, field_names))   # 👈 here
                    | f"DebugBeforeBQ_{table_name}" >> beam.Map(lambda r: print(f"FINAL {table_name} row: {r}"))
                    | f"CountRows_{table_name}" >> beam.Map(count_row)
                    | f"WriteToBigQuery_{table_name}" >> beam.io.WriteToBigQuery(
                        table_spec,
                        schema=bq_schema_str,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        
                    )
                )


if __name__ == "__main__":
    run_pipeline(
        "../config/table_config.json",
        "../config/bq_schemas.json",
        "../config/dataflow_options.json"
    )
