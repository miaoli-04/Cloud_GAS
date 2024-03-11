# archive_app.py
#
# Archive Free user data
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import requests
import sys
import time

from flask import Flask, request, jsonify

app = Flask(__name__)
app.url_map.strict_slashes = False


# Get configuration and add to Flask app object
environment = "archive_app_config.Config"
app.config.from_object(environment)
REGION_NAME = app.config["AWS_REGION_NAME"]
s3_client = boto3.client('s3', region_name = REGION_NAME)
glacier_client = boto3.client('glacier', region_name = REGION_NAME)
dynamo = boto3.resource('dynamodb', region_name=REGION_NAME)
ann_table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
sns_client = boto3.client('sns', region_name = REGION_NAME)
sqs_client = boto3.client('sqs', region_name = REGION_NAME)
WAIT_TIME = int(app.config["WAIT_TIME"])
MAX_MESSAGE = int(app.config["MAX_MESSAGE"])

sys.path.append(app.config['HOME_PATH'])
from gas.util.helpers import get_user_profile
from botocore.exceptions import ClientError

sys.path.append(app.config['HOME_PATH'])
from gas.util.helpers import get_user_profile
from botocore.exceptions import ClientError

@app.route("/", methods=["GET"])
def home():
    return f"This is the Archive utility: POST requests to /archive."

'''
reference:
    1. subscribe:https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/subscribe.html
        https://docs.aws.amazon.com/sns/latest/dg/SendMessageToHttp.prepare.html
    2. glacier: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
    3. delete from s3: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
    4. state machine: https://docs.aws.amazon.com/step-functions/latest/dg/tutorial-creating-lambda-state-machine.html
'''

@app.route("/archive", methods=["POST"])
def archive_free_user_data():
    print('searching...')
    try:
        messages = sqs_client.receive_message(QueueUrl = app.config['AWS_SQS_JOB_RESULT'],
                            WaitTimeSeconds= WAIT_TIME,
                            MaxNumberOfMessages= MAX_MESSAGE)

    except Exception as e:
        raise Exception(f'Error when receiving message')
    
    if 'Messages' in messages:
        for mes in messages['Messages']:
            post_req = json.loads(mes['Body'])
            print(post_req)
    
        if post_req.get('Type') == 'SubscriptionConfirmation':
                topicArn = post_req.get('TopicArn')
                token = post_req.get('Token')
                sns_client.confirm_subscription(TopicArn=topicArn, Token=token)

        elif post_req.get('Type') == 'Notification':
            message = json.loads(post_req.get('Message'))

            if message:
                result_file = message['s3_key_result_file']
                result_bucket = message["s3_result_bucket"]
                user_id = message['user_id']
                profile = get_user_profile(user_id)
                user_type = profile[4]

                #archive for free user
                if user_type == 'free_user':
                    try:
                        archive_id, upload = archive(result_bucket, result_file)
                        if upload:
                            clean_up(message, result_bucket, result_file, archive_id)
                        print('archive completed')
                    except Exception as e:
                        print(f'error: {e}')

                receipt_handle = mes['ReceiptHandle']
                try:
                    sqs_client.delete_message(QueueUrl=app.config['AWS_SQS_JOB_RESULT'], ReceiptHandle=receipt_handle)

                except ClientError as ce:
                    print(f'Fail to delete message: {ce}')

                except Exception as e:
                    print(f'Unexpected error: {e}')
                
                print('completed')

    return jsonify('hi')

def archive(result_bucket, result_file):
    upload = False
    #get input file from s3
    try:
        print(result_bucket, result_file)
        s3_file = s3_client.get_object(Bucket = result_bucket,
                                Key = result_file)
        body = s3_file['Body'].read()
        if body:
            upload = True

    except ClientError as err:
        print(f'Fail to update data: {err}')
        return err

    except Exception as e:
        print(f'Unexpected error when getting input: {e}')
        return err

    #upload to glacier
    try: 
        gla_response = glacier_client.upload_archive(vaultName = app.config['AWS_GLACIER_VAULT'], body=body)
        archive_id = gla_response['archiveId']
        print(f'{result_file} archived, archive id {archive_id}')

    except ClientError as err:
        raise ClientError(f'Fail to update data: {err}')

    except Exception as e:
        raise Exception(f'Unexpected error: {e}')

    return archive_id, upload


def clean_up(message, result_bucket, result_file, archive_id):
    #update DynamoDB
    print(message['job_id'],archive_id)
    try:
        ann_table.update_item(Key = {"job_id": message['job_id']},
                                UpdateExpression = 'SET results_file_archive_id =:a_id',
                                ExpressionAttributeValues={":a_id": archive_id})

    except ClientError as err:
        raise ClientError(f'Fail to update data: {err}')

    except Exception as e:
        raise Exception(f'Unexpected error: {e}')

    #delete file from s3
    try:
        s3_client.delete_object(Bucket = result_bucket,
                            Key = result_file)

    except ClientError as err:
        print(f'Fail to delete data: {err}')

    except Exception as e:
        print(f'Unexpected error when deleting: {e}')

### EOF