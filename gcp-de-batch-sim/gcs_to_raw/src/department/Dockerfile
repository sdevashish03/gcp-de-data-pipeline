# Use Google's official Dataflow Python base image
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Copy your pipeline code into the container
COPY department_pipeline.py .

# Set the entrypoint to run your pipeline
ENTRYPOINT ["python", "department_pipeline.py"]