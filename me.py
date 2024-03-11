import boto3
user_id = 'e3bd1406-c392-4262-8063-fc929bfb42c7'
archive_id = 'dStvJi5Y3BmGZ6wsdfqtnz2_NFI4hg_s6Js-b23FZMZRgq7VYsiZNUzCRCYRJKpKYihWBYMsbJJWbw5A32YDDFPbNsKz395IkexuMUZIkWeHbg7VTFHVCRvIufoTNNVTBa43ol2xeQ'
dynamo = boto3.resource('dynamodb', region_name="us-east-1")
ann_table = dynamo.Table('mli628_annotations')

def func(user_id, archive_id):
    #getting the job info
    response = ann_table.query(
            IndexName = 'user_id_index', 
            ProjectionExpression = 'results_file_archive_id, s3_key_result_file, s3_results_bucket, job_id',
            KeyConditionExpression = 'user_id = :user',
            FilterExpression = 'results_file_archive_id = :arid',
            ExpressionAttributeValues = {':user': user_id, ':arid':archive_id})
    
    
    job = response['Items'][0]
    print(job)

def fun():
    t = 'mi'
    ar = 123
    if ar and not t:
        print('hi')

if __name__ == '__main__':
    fun()