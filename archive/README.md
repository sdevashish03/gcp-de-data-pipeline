data-engineering-project/
│── dataflows/
│   ├── gcs_to_raw/              # Ingests data from GCS into Raw BigQuery tables
│   │   ├── src/                 # Apache Beam pipeline logic
│   │   │   ├── pipeline.py      # Beam transformations & pipeline setup
│   │   │   ├── utils.py         # Helper functions for parsing/validation
│   │   ├── schema/              # Schema definitions for Raw layer
│   │   ├── config/              # Configurations (JSON/YAML)
│   │   ├── tests/               # Unit & integration tests
│   │   ├── logs/                # Log files for monitoring/debugging
│   │   ├── README.md            # Documentation for GCS to Raw
│
│   ├── raw_to_staging/          # Cleansing, deduplication, enrichment
│   │   ├── src/                 # Apache Beam transformation logic
│   │   │   ├── pipeline.py      # Staging transformations
│   │   │   ├── validators.py    # Data validation methods
│   │   ├── schema/              # Schema definitions for Staging layer
│   │   ├── config/              # Configurations (JSON/YAML)
│   │   ├── tests/               # Unit & integration tests
│   │   ├── logs/                # Log files for monitoring/debugging
│   │   ├── README.md            # Documentation for Raw to Staging
│
│   ├── staging_to_curation/     # Business logic implementation, final processing
│   │   ├── src/                 # Apache Beam transformation logic
│   │   │   ├── pipeline.py      # Curation transformations
│   │   │   ├── calculations.py  # Derived metrics & calculated fields
│   │   ├── schema/              # Schema definitions for Curation layer
│   │   ├── config/              # Configurations (JSON/YAML)
│   │   ├── tests/               # Unit & integration tests
│   │   ├── logs/                # Log files for monitoring/debugging
│   │   ├── README.md            # Documentation for Staging to Curation
│
│── composer/                    # Airflow DAGs for pipeline orchestration
│   ├── dags/
│   │   ├── gcs_to_raw_dag.py    # DAG for triggering GCS to Raw pipeline
│   │   ├── raw_to_staging_dag.py # DAG for Raw to Staging pipeline
│   │   ├── staging_to_curation_dag.py # DAG for Staging to Curation pipeline
│   ├── plugins/                 # Custom Airflow hooks/operators
│   ├── config/                   # DAG configurations (YAML/JSON)
│   ├── tests/                    # DAG unit tests
│   ├── logs/                     # Composer logs
│   ├── README.md                 # Documentation for Composer orchestration
│
│── configs/
│   ├── env_vars.yaml             # Environment variables for pipelines
│   ├── gcp_resources.json        # Terraform definitions for GCP setup
│
│── scripts/
│   ├── setup_env.sh              # Bash script to set up environment variables
│   ├── deploy_dataflow.sh        # Script to deploy Dataflow jobs
│   ├── deploy_composer.sh        # Script to deploy Composer DAGs
│
│── docs/                         # Documentation, architecture diagrams
 
