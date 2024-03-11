# annotator.py
#
# NOTE: This file lives on the AnnTools instance
#
# Copyright (C) 2013-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import os
import sys
import time
from subprocess import Popen, PIPE
from botocore.exceptions import ClientError
from flask import abort

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")

DATA_PATH = config.get('ann','DATA_PATH')
s3 = boto3.client('s3')
cnet_id = config.get('DEFAULT', 'CnetId')

REGION_NAME = config.get('aws','AwsRegionName')
TABLE_NAME = config.get('db','ANN_TABLE')
dynamo = boto3.resource('dynamodb', region_name=REGION_NAME)
ann_table = dynamo.Table(TABLE_NAME)
sqs = boto3.client('sqs', region_name = REGION_NAME)
QUEUE_URL = config.get('sqs','QUEUE_URL')

"""Reads request messages from SQS and runs AnnTools as a subprocess.

Move existing annotator code here
"""
'''
reference：
1. configparser: https://docs.python.org/3/library/configparser.html
'''

def handle_requests_queue(sqs=None):
    rv = {}

    try: 
        print('Receiving message ...')
        messages = sqs.receive_message(QueueUrl = QUEUE_URL,
                            MaxNumberOfMessages= int(config.get('sqs', 'MaxMessages')),
                            WaitTimeSeconds= int(config.get('sqs', 'WaitTime')))

    except ClientError as ce:
        print(f'Fail to receive the message {ce}')
        return 'message not found'

    except Exception as e:
        print(f'Unexpected error when receiving message {e}')
        return 'message not found'

    if 'Messages' in messages:
        for message in messages['Messages']:

            try:
                filename, job_id, user_id = handle_message(message)
            except Exception as e:
                print(e)
                
            #run anntools
            response = run_anntools(filename, job_id, user_id)

            #delete message
            delete_message(message)


def main():

    # Get handles to queue

    # Poll queue for new results and process them
    while True:

        try:
            handle_requests_queue(sqs)

        except ClientError as ce:
            print(f'Fail to poll and process message {ce}')
            continue

        except Exception as e:
            print(f'Unexpected error: {e}')

def handle_message(message):
    rv = {}
    # Extract the SNS message content from the SQS message
    data = json.loads(json.loads(message['Body'])['Message'])
    job_id = data['job_id']
    user_id = data['user_id']
    file_name = data['input_file_name']
    bucket = data['s3_inputs_bucket']
    key = data['s3_key_input_file']

    #filepath
    folder_path = DATA_PATH + user_id + '/' + job_id + '/'

    #check file type
    if file_name[-4:] != '.vcf':
        rv['Code'] = 500
        rv['status'] = 'error'
        rv['message'] = f'Wrong file type: .vcf needed'
        print(json.dumps(rv))

    if not os.path.isdir(folder_path): 
        try:
            os.makedirs(folder_path)

        except OSError as oe:
            rv['Code'] = 500
            rv['status'] = 'error'
            rv['message'] = f'fail to make directory for file: {oe}'
            print(json.dumps(rv))
            
        except Exception as e:
            rv['Code'] = 500
            rv['status'] = 'error'
            rv['message'] = f'Unexpected Error: {e}'
            print(json.dumps(rv))

    filename =  folder_path + file_name

    #download file from s3
    try:
        s3.download_file(bucket, key, filename)

    except ClientError as fe:
        rv['Code'] = 500
        rv['status'] = 'error'
        rv['message'] = f'File not found: {fe}'
        print(json.dumps(rv))

    except Exception as e:
        rv['Code'] = 500
        rv['status'] = 'error'
        rv['message'] = f'Unexpected Error: {e}'
        print(json.dumps(rv))

    return filename, job_id, user_id

def run_anntools(filename, job_id, user_id):
    '''
    Run anntools and update job status
    input:
        filename: name of file in local document
        job_id: job_id of the file
    output:
        rv: result of running anntools
    '''
    rv = {'code': None, 'status': None}

    #running anntools
    try:
        p = Popen(['python', 'run.py', filename, job_id])

    except Exception as e:
        print('Fail to run anntools')

    #updating and checking status
    try:
        ann_table.update_item(Key = {"job_id": job_id},
                          UpdateExpression = 'SET job_status = :st',
                          ConditionExpression="job_status = :pd",
                          ExpressionAttributeValues={":st": 'RUNNING', ":pd": "PENDING"})

    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            print(f'File already annotated')

        else:
            print(f'{err}')

    except Exception as e:
        print(f'Unexpected Error: {e}')

    rv['Code'] = 200
    rv['status'] = 'success'
    rv['data'] = {}
    rv['data']['job_id'] = job_id
    rv['data']['input_file'] = filename.replace(cnet_id, '')


    return json.dumps(rv), 200

def delete_message(message):
    
    receipt_handle = message['ReceiptHandle']

    try:
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)

    except ClientError as ce:
        print(f'Fail to delete message: {ce}')

    except Exception as e:
        print(f'Unexpected error: {e}')


if __name__ == "__main__":
    main()

### EOF