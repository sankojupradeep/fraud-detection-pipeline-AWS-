import boto3
import json
import time

glue = boto3.client('glue', region_name='us-east-1')

def lambda_handler(event, context):
    print(f"Event received: {json.dumps(event)}")

    #Get Uploaded File Details 
    bucket = event['detail']['bucket']['name']
    key    = event['detail']['object']['key']

    print(f"New file uploaded: s3://{bucket}/{key}")

    #Only Process Transaction Files 
    if 'raw/transactions' not in key:
        print("Not a transaction file. Skipping.")
        return {"status": "skipped"}

    # JOB 1 — Transformation-1 (Clean Job)
    clean_run_id = None
    try:
        clean_response = glue.start_job_run(JobName='Transformation-1')
        clean_run_id   = clean_response['JobRunId']
        print(f"Started Transformation-1 | Run ID: {clean_run_id}")
    except Exception as e:
        print(f"Failed to start Transformation-1: {e}")
        raise e

    # Transformation-1
    print("Waiting for Transformation-1 to complete...")
    while True:
        response = glue.get_job_run(
            JobName='Transformation-1',
            RunId=clean_run_id
        )
        status = response['JobRun']['JobRunState']
        print(f"Transformation-1 status: {status}")

        if status == 'SUCCEEDED':
            print("Transformation-1 SUCCEEDED — starting Transformation-2...")
            break
        elif status in ['FAILED', 'ERROR', 'TIMEOUT']:
            print(f"Transformation-1 FAILED with status: {status}")
            return {
                "status"       : "failed",
                "failed_job"   : "Transformation-1",
                "job_status"   : status,
                "clean_run_id" : clean_run_id
            }

        time.sleep(30)

    # JOB 2 — Transformation-2 (Fraud Rules Job)
    rules_run_id = None
    try:
        rules_response = glue.start_job_run(JobName='Transformation-2')
        rules_run_id   = rules_response['JobRunId']
        print(f"Started Transformation-2 | Run ID: {rules_run_id}")
    except Exception as e:
        print(f"Failed to start Transformation-2: {e}")
        raise e

    #Wait for Transformation-2 
    print("Waiting for Transformation-2 to complete...")
    while True:
        response = glue.get_job_run(
            JobName='Transformation-2',
            RunId=rules_run_id
        )
        status = response['JobRun']['JobRunState']
        print(f"Transformation-2 status: {status}")

        if status == 'SUCCEEDED':
            print("Transformation-2 SUCCEEDED — pipeline complete!")
            break
        elif status in ['FAILED', 'ERROR', 'TIMEOUT']:
            print(f"Transformation-2 FAILED with status: {status}")
            return {
                "status"       : "failed",
                "failed_job"   : "Transformation-2",
                "job_status"   : status,
                "rules_run_id" : rules_run_id
            }

        time.sleep(30)

    print("Pipeline completed successfully!")
    return {
        "status"       : "success",
        "file"         : key,
        "bucket"       : bucket,
        "clean_run_id" : clean_run_id,
        "rules_run_id" : rules_run_id
    }