# GAS Project based on Cloud
### Introductions
This project used AWS to build a web application to robustly process genomic annotation requests from users
### Files
* **View.py**: The front end of the website that is based on flask. It uses SNS to send annotation request, DynamoDB to store request information, and S3 to keep input file
* **annotator.py**: The script that implement the annotation with anntools. It uses the long polling to process messages from SQS and upload result files to S3
* **archive_app.py**: The script establishes a webhook which works together with a state machine. For free users, their result file will be archived in Glaicer three minutes the annotation is completed. By then, the State Machine will send a message to sns (and thus sqs), where archive to poll and process the message 
* **thaw.py**: A webhook to send retrieve request to glacier. If the free user update to premium user, View.py will send thaw-request to retrieve the file from glacier to the thaw endpoint.
* **lambda.py**: The script for restore files to S3. This function is deployed on AWS Lambda, and when Glacier successfully thaw the file, this script will restore it to the corresponding S3 and delete archive in Glaicer.
