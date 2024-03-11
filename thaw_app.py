# thaw_app.py
#
# Thaws upgraded (Premium) user data
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import requests
import sys
import time

from botocore.exceptions import ClientError
from flask import Flask, request

app = Flask(__name__)
app.url_map.strict_slashes = False

# Get configuration and add to Flask app object
environment = "thaw_app_config.Config"
app.config.from_object(environment)
REGION_NAME = app.config["AWS_REGION_NAME"]
WAIT_TIME = int(app.config["WAIT_TIME"])
MAX_MESSAGE = int(app.config["MAX_MESSAGE"])
s3_client = boto3.client('s3', region_name = REGION_NAME)
glacier_client = boto3.client('glacier', region_name = REGION_NAME)
sns_client = boto3.client('sns', region_name = REGION_NAME)
sqs_client = boto3.client('sqs', region_name = REGION_NAME)
dynamo = boto3.resource('dynamodb', region_name=REGION_NAME)
ann_table = dynamo.Table(app.config['TABLE_NAME'])
EXPEDITED = app.config["TIER_EX"]
STANDARD = app.config["TIER_ST"]



@app.route("/", methods=["GET"])
def home():
    return f"This is the Thaw utility: POST requests to /thaw."

'''
reference:
    1.retore object in glacier: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/restore_object.html
    2. initiate archive retrival: https://docs.aws.amazon.com/amazonglacier/latest/dev/example_glacier_InitiateJob_ArchiveRetrieval_section.html
    3. describe job: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/describe_job.html
    4. provisioned capability for glaicer retrival: https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-two-steps.html#api-downloading-an-archive-two-steps-retrieval-expedited-capacity
'''
@app.route("/thaw", methods=["POST"])
def thaw_premium_user_data():
    try:
        post_req = request.get_json(force=True)

    except Exception as e:
        print('Error in receiving request')
        return e
    
    if post_req.get('Type') == 'SubscriptionConfirmation':
            topicArn = post_req.get('TopicArn')
            token = post_req.get('Token')
            sns_client.confirm_subscription(TopicArn=topicArn, Token=token)
    
    elif post_req.get('Type') == 'Notification':
        messages = sqs_client.receive_message(QueueUrl = app.config['THAW_SQS'],
                        WaitTimeSeconds= WAIT_TIME,
                        MaxNumberOfMessages= MAX_MESSAGE)
        print('message get')
            
        if 'Messages' in messages:
            for message in messages['Messages']:
                print('processing request...')
                response = send_thaw_request(message)

                if response == 'All request finished':
                    #delete
                    receipt_handle = message['ReceiptHandle']
                    try:
                        sqs_client.delete_message(QueueUrl=app.config['THAW_SQS'], ReceiptHandle=receipt_handle)

                    except ClientError as ce:
                        print(f'Fail to delete message: {ce}')

                    except Exception as e:
                        print(f'Unexpected error: {e}')
                    
                    print('completed')

        return json.dumps('message published')
                
def send_thaw_request(message):

    data = json.loads(json.loads(message['Body'])['Message'])
    user_id = data['user_id']
    arc_lst = get_arc_ids(user_id)

    if len(arc_lst) == 0:
        print('no files to retrieve')
        return 'no files to retrieve'

    for archive_id, job_id in arc_lst:
        print('sending request...')
        print(job_id, archive_id)

        #try expedited
        try:
            arc_job_id = glacier_retrival(archive_id, user_id, tier = EXPEDITED)
            print('Expedited request sent')

        except ClientError as ce:
            
            #if expedited not work due to provision capacity, try the standard approach
            if ce.response['Error']['Code'] == 'InsufficientCapacityException':
                print('Expedited retrival failed: now trying standard retrival')

                try:
                    arc_job_id = glacier_retrival(archive_id, user_id, tier = STANDARD)
                    print('Standard request sent')

                except ClientError as ce:
                    print(f'standard failed: {ce}')
                    return ce

                except Exception as e:
                    print(f'failed to initate unexpectedly {e}')
                    return e
                
                response = glacier_client.describe_job(
                    vaultName=app.config['AWS_GLACIER_VAULT'],
                    jobId=arc_job_id)
                
                # status_check(response)
                update_request_sent(job_id)
                continue
        
                
            return ce
        
        except Exception as e:
            print(f'failed to initate unexpectedly {e}')
            return e
        
        response = glacier_client.describe_job(
                    vaultName=app.config['AWS_GLACIER_VAULT'],
                    jobId=arc_job_id)
        
        # status_check(response) 
        update_request_sent(job_id)

        
    return 'All request finished'       
           
def status_check(response):
    while response['StatusCode'] != 'Succeeded':
        print('status check',response['StatusCode'])
        time.sleep(60)

def glacier_retrival(archive_id, user_id, tier):
    retrival_job = glacier_client.initiate_job(
                                vaultName=app.config['AWS_GLACIER_VAULT'],
                                jobParameters={'Type': 'archive-retrieval',
                                                'ArchiveId': archive_id,
                                                'Description': user_id,
                                                'Tier': tier,
                                                'SNSTopic': app.config['AWS_SNS_JOB_RESTORE_TOPIC']})
    job_id = retrival_job['jobId']

    return job_id

def get_arc_ids(user_id):
    response = ann_table.query(
            IndexName = 'user_id_index', 
            ProjectionExpression = 'results_file_archive_id, retrival_request_sent, job_id',
            KeyConditionExpression = 'user_id = :user',
            ExpressionAttributeValues = {':user': user_id})
    
    arc_lst = []
    for job in response['Items']:
        arc_id = job.get('results_file_archive_id')
        req_sent = job.get('retrival_request_sent')
        job_id = job.get('job_id')
        if arc_id and not req_sent: #send request only when the request is not sent and archive id still exist
            arc_lst.append((arc_id, job_id))

    return arc_lst

def update_request_sent(job_id):
    try:
        ann_table.update_item(Key = {"job_id": job_id},
                              UpdateExpression = 'SET retrival_request_sent =:st',
                              ExpressionAttributeValues={":st": True})
        print('Thaw request sent', job_id)
        
    except ClientError as ce:
        print(f'Failed to update request status:{ce}')
        return ce

    except Exception as e:
        print(f'Failed to update request status id unexpectedly:{e}')
        return e


            


### EOFs
