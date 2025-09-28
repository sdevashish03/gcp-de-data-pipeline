import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv


def load_json(path):
    with open(path) as f:
        return json.load(f)


def schema_to_bq_string(schema_list):
    return ",".join(f"{field.get('name', '')}:{field.get('type', '')}" for field in schema_list)


def extract_field_properties(schema_list):
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


def parse_csv_line(line, field_names):
    values = next(csv.reader([line]))
    if len(values) < len(field_names):
        values.extend([None] * (len(field_names) - len(values)))
    return dict(zip(field_names, values))


def clean_row(row, nullable_fields):
    if row is None:
        return None
    return {k: (None if (v in ("NULL", "")) and k in nullable_fields else v) for k, v in row.items()}


def validate_row(row, required_fields, field_names, table_name):
    if row is None:
        print(f"{table_name}: Dropping None row")
        return False
    if set(row.keys()) != set(field_names):
        print(f"{table_name}: Dropping row with unexpected fields: {row}")
        return False
    for field in required_fields:
        val = row.get(field)
        if val is None or val == '':
            print(f"{table_name}: Dropping row missing required '{field}': {row}")
            return False
    return True


def run(project_id, dataset_id, table_name, input_path, config_path, schema_path, options_path):
    print(f"Running pipeline with table_name={table_name}, input_path={input_path}")
    config = load_json(config_path)
    schemas = load_json(schema_path)
    options_dict = load_json(options_path)
    pipeline_options = PipelineOptions(**options_dict)

    schema = schemas[table_name]
    field_names, required_fields, nullable_fields = extract_field_properties(schema)
    bq_schema_str = schema_to_bq_string(schema)
    table_spec = f"{project_id}:{dataset_id}.{table_name}"

    rows_processed_counter = beam.metrics.Metrics.counter('pipeline', f'{table_name}_rows_processed')

    def count_row(row):
        rows_processed_counter.inc()
        return row

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | f"Read_{table_name}" >> beam.io.ReadFromText(input_path, skip_header_lines=1)
            | f"Parse_{table_name}" >> beam.Map(lambda r: parse_csv_line(r, field_names))
            | f"Clean_{table_name}" >> beam.Map(lambda r: clean_row(r, nullable_fields))
            | f"FilterNone_{table_name}" >> beam.Filter(lambda r: r is not None)
            | f"Validate_{table_name}" >> beam.Filter(lambda r: validate_row(r, required_fields, field_names, table_name))
            | f"CountRows_{table_name}" >> beam.Map(count_row)
            | f"WriteToBigQuery_{table_name}" >> beam.io.WriteToBigQuery(
                    table_spec,
                    schema=bq_schema_str,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    custom_gcs_temp_location=options_dict.get("temp_location"),
                )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Dataflow pipeline for a specific table")
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--dataset_id", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--config_path", default="../config/table_config.json")
    parser.add_argument("--schema_path", default="../config/bq_schemas.json")
    parser.add_argument("--options_path", default="../config/dataflow_options.json")

    args = parser.parse_args()

    run(
        args.project_id,
        args.dataset_id,
        args.table_name,
        args.input_path,
        args.config_path,
        args.schema_path,
        args.options_path,
    )
