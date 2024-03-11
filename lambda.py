import json
import boto3
dynamo = boto3.resource('dynamodb', region_name="us-east-1")
ann_table = dynamo.Table('mli628_annotations')
glacier_client = boto3.client('glacier', region_name ="us-east-1")
s3_client = boto3.client('s3', region_name = "us-east-1")
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
sqs_client = boto3.client('sqs', region_name = "us-east-1")
RESTORE_SQS = "https://sqs.us-east-1.amazonaws.com/127134666975/mli628-a16-restore"
'''
get_job_output: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
scan: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/scan.html
delete archive: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
'''

def lambda_handler(event, context):
    
    archive_del = False
    messages = sqs_client.receive_message(QueueUrl = RESTORE_SQS,
                            MaxNumberOfMessages= 10)
                            
    if 'Messages' in messages:
        
        for mes in messages['Messages']:
            receipt_handle = mes['ReceiptHandle']

            sqs_message = json.loads(mes['Records'][0]['body'])
            message = json.loads(sqs_message['Message'])

            #getting variables
            print(message)
            archive_id = message.get('ArchiveId')
            vault_arn = message.get('VaultARN')
            vault_name = vault_arn.split(':')[-1].split('/')[-1]
            archive_job_id = message.get('JobId')
            user_id = message.get('JobDescription')

            try:
                job_id, bucket, file_key = restore_file(user_id, vault_name, archive_job_id)
            except Exception as e:
                raise Exception("Unexpected error when restoring file")


            #clean up
            try:
                clean_up(bucket, file_key, vault_name, archive_id, job_id)
            except Exception as e:
                raise Exception("Unexpected error when cleaning up file")
            
            #delete message from sqs
            try:
                sqs_client.delete_message(QueueUrl=RESTORE_SQS, ReceiptHandle=receipt_handle)

            except ClientError as ce:
                print(f'Fail to delete message: {ce}')

            except Exception as e:
                print(f'Unexpected error: {e}')
            
            print('completed')


def restore_file(user_id, vault_name, archive_job_id):
    
    #getting the job info
    response = ann_table.query(
            IndexName = 'user_id_index', 
            ProjectionExpression = 'results_file_archive_id, s3_key_result_file, s3_results_bucket, job_id',
            KeyConditionExpression = 'user_id = :user',
            ExpressionAttributeValues = {':user': user_id})
            
    job = response['Items'][0]
    file_key = job.get('s3_key_result_file')
    bucket = job.get('s3_results_bucket')
    job_id = job.get('job_id')
    
    retri_file = glacier_client.get_job_output(
                    vaultName=vault_name,
                    jobId=archive_job_id)
    
    #restore the file
    file = retri_file.get('body').read()
    try:
        s3_client.put_object(Bucket=bucket, Key=file_key, Body=file)
        
    except ClientError as ce:
        print(ce)
        
    except Exception as e:
        print(e)
    
    return job_id, bucket, file_key
    
def clean_up(bucket, file_key, vault_name, archive_id, job_id):
     #if the file is restored, delete archive_id and archive file
    restore_file = s3_client.get_object(Bucket=bucket, Key=file_key)

    if restore_file.get('Body'):
        try:
            glacier_client.delete_archive(
                vaultName=vault_name,
                archiveId=archive_id)
            archive_del = True
                
        except ClientError as ce:
            print(f'Failed to delete archived file:{ce}')
        
        except Exception as e:
            print(f'Failed to delete archived file unexpectedly:{e}')
        
        if archive_del:
            try:
                ann_table.update_item(Key = {"job_id": job_id},
                          UpdateExpression = 'Remove results_file_archive_id')
                
            except ClientError as ce:
                print(f'Failed to remove archived id:{ce}')
        
            except Exception as e:
                print(f'Failed to remove archived id unexpectedly:{e}')

    
        