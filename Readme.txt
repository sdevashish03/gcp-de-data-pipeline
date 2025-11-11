--To activate service account

gcloud auth activate-service-account gcp-de-batch-sim-5-sa@gcp-de-batch-sim-5.iam.gserviceaccount.com --key-file="D:/Learning/GCP Data Engineer/PDE Projects/json_key/gcp-de-batch-sim/gcp-de-batch-sim-5-456c9bf832ac.json"


Final Fix: Unset the Environment Variable
To ensure your pipeline uses the correct credentials (application_default_credentials.json), run this in PowerShell:

---Remove-Item Env:GOOGLE_APPLICATION_CREDENTIALS

Or in CMD:
---set GOOGLE_APPLICATION_CREDENTIALS=


This will remove the override and let Beam use the default credentials you just configured via gcloud auth application-default login.



--Switch the service account and the project
--1. gcloud auth list
--2. gcloud config list
--3. --Change Active project
    --gcloud config set project [YOUR_PROJECT_ID] (--gcp-de-batch-sim-464816-476514)
--


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
