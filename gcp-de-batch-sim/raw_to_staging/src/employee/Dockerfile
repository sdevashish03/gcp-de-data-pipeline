FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Install dependencies directly
RUN pip install apache-beam[gcp]==2.53.0 \
    google-cloud-storage \
    google-cloud-bigquery

# Copy pipeline code
COPY . /dataflow/template/
WORKDIR /dataflow/template

ENTRYPOINT ["python", "raw_to_stg_employee_pipeline.py"]