import boto3
import time

glue = boto3.client('glue')
sns = boto3.client('sns')

glue_jobs = [
    'retail_shipping_method',
    'retail_payment_methods',
    'retail_income',
    'retail_gender',
    'retail_location',
    'retail_order_status',
    'retail_feedback',
    'retail_product',
    'retail_customer',
    'retail_sales_full_load'
]

sns_topic_arn = ''

def send_sns(subject, message):
    sns.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=message
    )

def wait_for_job_completion(job_name, job_run_id):
    while True:
        status = glue.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState']
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            return status
        time.sleep(30)

def lambda_handler(event, context):
    for job_name in glue_jobs:
        try:
            # Start the Glue job
            response = glue.start_job_run(JobName=job_name)
            job_run_id = response['JobRunId']
            send_sns(f"{job_name} Started", f"Glue Job {job_name} has started. Job Run ID: {job_run_id}")
            
            # Wait for job completion
            job_status = wait_for_job_completion(job_name, job_run_id)
            
            if job_status == 'SUCCEEDED':
                send_sns(f"{job_name} Succeeded", f"Glue Job {job_name} completed successfully.")
            else:
                send_sns(f"{job_name} Failed", f"Glue Job {job_name} failed. Stopping execution.")
                raise Exception(f"{job_name} failed.")
        
        except Exception as e:
            send_sns("Glue Job Execution Stopped", str(e))
            return {
                'statusCode': 500,
                'body': f"Job execution stopped due to failure in {job_name}."
            }

    send_sns("All Glue Jobs Completed", "All Glue jobs executed successfully in order.")
    return {
        'statusCode': 200,
        'body': "All Glue jobs executed successfully."
    }