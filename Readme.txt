--To create a docker image
--Step 1
1st file
gcloud builds submit --tag gcr.io/gcp-de-batch-sim-464816-476514/department_pipeline .

gcloud builds submit --tag gcr.io/gcp-de-batch-sim-464816-476514/department_rts_pipeline .

2nd file
cd ../employee
gcloud builds submit --tag gcr.io/gcp-de-batch-sim-464816-476514/employee_pipeline .

gcloud builds submit --tag gcr.io/gcp-de-batch-sim-464816-476514/employee_rts_pipeline .

--Step 2 To create flex template its a json
--for department
gcloud dataflow flex-template build gs://us-central1-gcp-de-batch-si-4fe23add-bucket/templates/department_pipeline_flex.json --image gcr.io/gcp-de-batch-sim-464816-476514/department_pipeline --sdk-language "PYTHON" --metadata-file metadata_department.json

gcloud dataflow flex-template build gs://us-central1-gcp-de-batch-si-4fe23add-bucket/templates/department_pipeline_rts_flex.json --image gcr.io/gcp-de-batch-sim-464816-476514/department_pipeline_rts --sdk-language "PYTHON" --metadata-file metadata_department.json

--for employee
gcloud dataflow flex-template build gs://us-central1-gcp-de-batch-si-4fe23add-bucket/templates/employee_pipeline_flex.json --image gcr.io/gcp-de-batch-sim-464816-476514/employee_pipeline --sdk-language "PYTHON" --metadata-file metadata_employee.json

gcloud dataflow flex-template build gs://us-central1-gcp-de-batch-si-4fe23add-bucket/templates/employee_pipeline_rts_flex.json --image gcr.io/gcp-de-batch-sim-464816-476514/employee_pipeline_rts --sdk-language "PYTHON" --metadata-file metadata_employee.json
