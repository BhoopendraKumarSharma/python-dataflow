import shutil
import os
import subprocess

# Define the base configuration and parameters
base_input_path = "gs://bks-pipeline-test/cdr_L-SC4*"
base_job_name = "my-dataflow-job"
output_directory = "deployed_dataflows"

# Define the specific configurations for each cluster
cluster_configurations = [
    {
        "input_path": "gs://bucket1/cdr_L-SC4*",
        "job_name": "dataflow-job-1"
    },
    {
        "input_path": "gs://bucket2/cdr_L-SC4*",
        "job_name": "dataflow-job-2"
    },
    # Add configurations for the remaining clusters
]

# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

# Read the Dataflow Python code file
with open("dataflow.py", "r") as file:
    dataflow_code = file.read()

# Iterate over the cluster configurations
for index, config in enumerate(cluster_configurations):
    # Update the input_path and job_name in the Dataflow code
    updated_code = dataflow_code.replace(base_input_path, config["input_path"])
    updated_code = updated_code.replace(base_job_name, config["job_name"])

    # Write the updated code to a new file
    output_file = f"{output_directory}/dataflow_{index}.py"
    with open(output_file, "w") as file:
        file.write(updated_code)

    # Deploy the Dataflow job using the updated code
    deployment_command = [
        "python", output_file,
        "--project=vivid-argon-376508",
        "--region=us-east1",
        "--runner=DataflowRunner",
        "--staging_location=gs://bks-df-processing/tmp1/",
        "--temp_location=gs://bks-df-processing/tmp2/",
        "--machine_type=n1-highmem-4",
        "--save_main_session"
    ]
    subprocess.run(deployment_command)

    # Optionally, delete the output file
    # Uncomment the line below if you want to delete the output files after deployment
    # os.remove(output_file)

# Clean up the output directory
shutil.rmtree(output_directory)
