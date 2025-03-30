import boto3
import json
import os
import sys
import time
from face_recognition import face_match

ASU_ID = "1220103989"

# AWS resource names and clients
INPUT_BUCKET = f"{ASU_ID}-in-bucket"
OUTPUT_BUCKET = f"{ASU_ID}-out-bucket"
REQUEST_QUEUE = f"{ASU_ID}-req-queue"
RESPONSE_QUEUE = f"{ASU_ID}-resp-queue"
s3 = boto3.client('s3', region_name='us-east-1')
sqs = boto3.client('sqs', region_name='us-east-1')
REQUEST_QUEUE_URL = sqs.get_queue_url(QueueName=REQUEST_QUEUE)['QueueUrl']
RESPONE_QUEUE_URL = sqs.get_queue_url(QueueName=RESPONSE_QUEUE)['QueueUrl']

def handle_request(request):
    try:
        file_name = json.loads(request['Body']).get('filename')
        match_image = file_name.split('.')[0]
        tmp_path = os.path.join('/tmp', file_name)

        try:
            s3.download_file(INPUT_BUCKET, file_name, tmp_path)
            match_result = face_match(tmp_path, 'data.pt')[0]
            os.unlink(tmp_path)
        except Exception as e:
            print(f"Error processing image: {e}")
            
        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=match_image,
            Body=match_result
        )
        response_queue_message = {
            'filename': match_image,
            'result': match_result
        }
        sqs.send_message(
            QueueUrl=RESPONE_QUEUE_URL,
            MessageBody=json.dumps(response_queue_message)
        )
        sqs.delete_message(
            QueueUrl=REQUEST_QUEUE_URL,
            ReceiptHandle=request['ReceiptHandle']
        )
        return True
    except Exception as e:
        print(f"Error processing request: {e}")
        return False

def check_for_more_requests():
    req_message = sqs.receive_message(
        QueueUrl=REQUEST_QUEUE_URL,
        MaxNumberOfMessages=1,
        VisibilityTimeout=0,  
        WaitTimeSeconds=1     
    )

    return 'Messages' in req_message and len(req_message['Messages']) > 0

def main():
    request_processed = False
    try:
        req_message = sqs.receive_message(
            QueueUrl=REQUEST_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20  
        )

        if 'Messages' in req_message:
            request_processed = handle_request(req_message['Messages'][0])
        else:
            print("No messages in queue, exiting")
            return  
    except Exception as e:
        print(f"Error processing request: {e}")
    
    if request_processed:

        time.sleep(1)
        if check_for_more_requests():
            main() 
        else:
            print("exiting")
    else:
        print("Failed")
    
if __name__ == "__main__":
    main()
