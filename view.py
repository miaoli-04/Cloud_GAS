# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import uuid
import time
import json
from datetime import datetime, timedelta

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import abort, flash, redirect, render_template, request, session, url_for

from app import app, db
from decorators import authenticated, is_premium
import sys

REGION_NAME = app.config["AWS_REGION_NAME"]
TABLE_NAME = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
dynamo = boto3.resource('dynamodb', region_name=REGION_NAME)
ann_table = dynamo.Table(TABLE_NAME)
sns = boto3.client('sns', region_name = REGION_NAME)
s3 = boto3.client( "s3",
        region_name= REGION_NAME,
        config=Config(signature_version="s3v4"))

from auth import update_profile, get_profile

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    
    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session.get('primary_identity')

    # Generate unique ID to be used as S3 key (name)
    key_name = (app.config["AWS_S3_KEY_PREFIX"]+ user_id + "/"
        + str(uuid.uuid4()) + "~${filename}")

    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""],
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template(
        "annotate.html", s3_post=presigned_post, role=session["role"]
    )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route("/annotate/job", methods=["GET"])
@authenticated
def create_annotation_job_request():
    user_id = session.get('primary_identity')

    # Parse redirect URL query parameters for S3 object info
    bucket = request.args.get("bucket")
    s3_key = request.args.get("key")


    # Extract the job ID from the S3 
    _, user, file = s3_key .split('/')
    job_id, file_name = file.split('~')
    
    # Persist job to database
    data = { "job_id": job_id,
              "user_id": user_id, 
              "input_file_name": file_name, 
              "s3_inputs_bucket": bucket, 
              "s3_key_input_file": s3_key, 
              "submit_time": int(time.time()),
              "job_status": "PENDING"
            }
    #upload to DynamoDB
    try:
        ann_table.put_item(Item = data)

    except ClientError as ce:
        app.logger.exception(f"Error putting item to dynamoDB: '{job_id}'")
        return abort(500)
        
    except Exception as e:
        app.logger.exception(f"Error putting item to dynamoDB: '{job_id}'")
        return abort(500)

    # Send message to request queue
    try:
        print(app.config['AWS_SNS_JOB_REQUEST_TOPIC'])
        response = sns.publish(TopicArn = app.config['AWS_SNS_JOB_REQUEST_TOPIC'], 
                Message = json.dumps(data))
        print('message sent to request')

    except ClientError as ce:
        app.logger.exception(f"Error publishing job to sns: '{job_id}'")
        return abort(500)
        
    except Exception as e:
        app.logger.exception(f"Error publishing job to sns: '{job_id}'")
        return abort(500)

    return render_template("annotate_confirm.html", job_id=job_id)


"""List all annotations for the user
"""

'''
reference:
    1. boto3 query for dynamoDB: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
    2. code sample: https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/python/cross_service/dynamodb_item_tracker
    3. astimezone: https://stackoverflow.com/questions/5280692/python-convert-local-time-to-another-time-zone
    4. if condition in html: https://help.retentionscience.com/hc/en-us/articles/115003025814-How-To-Build-HTML-for-Conditional-Statements
    5. check length in html: https://stackoverflow.com/questions/1465249/get-lengths-of-a-list-in-a-jinja2-template
'''

@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():

    user_id = session.get('primary_identity')
    # Get list of annotations to display
    try:
        response = ann_table.query(
            IndexName = 'user_id_index', 
            ProjectionExpression = 'job_id, submit_time, input_file_name, job_status, user_id',
            KeyConditionExpression = 'user_id = :user',
            ExpressionAttributeValues = {':user': user_id})

    except ClientError as ce:
        app.logger.error(f'Unable to draw annotations: {ce}')
        if not user_id: #unauthenticated
            return abort(403)
        
        return abort(500)

    except Exception as e:
        app.logger.error(f'Unexpected error when drawing annotations:{e}')
        return abort(500)

    for item in response['Items']:
        item['submit_time'] = change_time_to_CST(item['submit_time'])

    url = request.url

    return render_template("annotations.html", annotations=response['Items'], url = url)


"""Display details of a specific annotation job
"""
'''
reference
    1. url for download: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html

'''

@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):
    
    res_url, status = None, None

    #check whether a job id exist
    response = ann_table.get_item(Key={'job_id': id})

    if 'Item' not in response:
        return abort(404)

    try:
        response = ann_table.query(
            ProjectionExpression = 'job_id, submit_time, input_file_name, job_status, user_id,\
                                    complete_time, s3_key_input_file, s3_key_result_file, \
                                    s3_key_log_file, results_file_archive_id, s3_results_bucket',
            KeyConditionExpression = 'job_id = :id',
            ExpressionAttributeValues = {':id': id})

    except ClientError as ce:
        app.logger.exception(f'error when finding job detail: {id}')

        if not session.get('primary_identity'): #unauthenticated
            return abort(401)

        return abort(500)

    except Exception as e:
        app.logger.exception(f'error when finding job detail: {id}')
        return abort(500)

    job = response['Items'][0]
    job_id = job['job_id']
    user_id = session.get('primary_identity')
    user_type = get_profile(identity_id = user_id).role

    if session.get('primary_identity') != job['user_id']:
        return abort(403)
    
    job['submit_time'] = change_time_to_CST(job['submit_time'])

    if job['job_status'] == 'COMPLETED':
        time_diff = (time.time() -  int(job['complete_time']))
        job['complete_time'] = change_time_to_CST(job['complete_time'])

        #check status
        if user_type == 'free_user' and time_diff > app.config['FREE_USER_DATA_RETENTION']:
            status = 'archive'
        elif user_type =='premium_user' and job.get('results_file_archive_id'):
            status = 'restore'
        
        #url to download res
        try:
            res_url = s3.generate_presigned_url('get_object',
                                          Params={'Bucket': app.config["AWS_S3_RESULTS_BUCKET"], 'Key': job['s3_key_result_file']},
                                                    ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'] )
        except ClientError as ce:
            app.logger.exception(f'error when generating presigned url for result file: {id}')
            return abort(500)

        except Exception as e:
            app.logger.exception(f'error when generating presigned url for result file: {id}')
            return abort(500)
        
        #initiate thawing process

    #url to download input file
    try:
        input_url = s3.generate_presigned_url('get_object',
                                          Params={'Bucket': app.config["AWS_S3_INPUTS_BUCKET"], 'Key': job['s3_key_input_file']},
                                                    ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'] )
    except ClientError as ce:
        app.logger.exception(f'error when generating presigned url for result file: {id}')
        return abort(500)

    except Exception as e:
        app.logger.exception(f'error when generating presigned url for result file: {id}')
        return abort(500)
    
    print('status',status)
    return render_template("annotation.html", job=job, input_url = input_url, 
                           res_url = res_url, status = status)

"""Display the log file contents for an annotation job
"""
'''
reference: 
    1. read s3 file: https://stackoverflow.com/questions/36205481/read-file-content-from-s3-bucket-with-boto3
'''

@app.route("/annotations/<id>/log", methods=["GET"])
@authenticated
def annotation_log(id):

    try:
        response = ann_table.query(
            ProjectionExpression = 'job_id, s3_key_log_file, user_id',
            KeyConditionExpression = 'job_id = :id',
            ExpressionAttributeValues = {':id': id})

    except ClientError as ce:
        app.logger.error(f'{ce}')
        if not session.get('primary_identity'): #unauthenticated
            return abort(401)
        return abort(500)

    except Exception as e:
        app.logger.error(f'{e}')
        return abort(500)

    job = response['Items'][0]

    #wrong user
    if session.get('primary_identity') != job['user_id']:
        return abort(403)
    
    try:
        response = s3.get_object(Bucket=app.config["AWS_S3_RESULTS_BUCKET"], Key=job['s3_key_log_file'])
        content = response['Body'].read().decode('utf-8')
    
    except KeyError:
        app.logger.exception(f'log file not found: {id}')
        return abort(404)
        
    except ClientError as ce:
        app.logger.exception(f'error when reading log: {id}')
        return abort(500)

    except Exception as e:
        app.logger.exception(f'error when reading log: {id}')
        return abort(500)

    return render_template("view_log.html", job_id = job['job_id'], content = content)

"""Subscription management handler
"""
import stripe

@app.route("/subscribe", methods=["GET", "POST"])
@authenticated
def subscribe():
    if request.method == "GET":
        user_id = session["primary_identity"]
        update_profile(identity_id= user_id, role="premium_user")

        thaw_data = { "user_id": user_id}

        try:
            print(app.config['AWS_SNS_JOB_THAW'])
            response = sns.publish(TopicArn = app.config['AWS_SNS_JOB_THAW'], 
                    Message = json.dumps(thaw_data))

        except ClientError as ce:
            app.logger.exception(f"Error publishing job to sns: '{user_id}'")
            return abort(500)
        
        except Exception as e:
            app.logger.exception(f"Error publishing job to sns: '{user_id}'")
            return abort(500)
        
        return render_template("subscribe_confirm.html", stripe_id = "forced_upgrade")

    elif request.method == "POST":
        # Process the subscription request

        # Create a customer on Stripe

        # Subscribe customer to pricing plan

        # Update user role in accounts database

        # Update role in the session

        # Request restoration of the user's data from Glacier
        # ...add code here to initiate restoration of archived user data
        # ...and make sure you handle files pending archive!

        # Display confirmation page
        pass

def change_time_to_CST(time):
    timestamp = int(time)
    return datetime.utcfromtimestamp(timestamp) + timedelta(hours=-6)


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Set premium_user role
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )


### EOF