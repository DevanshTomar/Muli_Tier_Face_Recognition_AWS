import boto3
import json
import os
import uuid
from flask import Flask, request, Response
import threading
import time

ASU_ID = "1220103989"
INPUT_BUCKET = f"{ASU_ID}-in-bucket"
OUTPUT_BUCKET = f"{ASU_ID}-out-bucket"
REQUEST_QUEUE = f"{ASU_ID}-req-queue"
RESPONSE_QUEUE = f"{ASU_ID}-resp-queue"
s3 = boto3.client('s3', region_name='us-east-1')
sqs = boto3.client('sqs', region_name='us-east-1')
REQUEST_QUEUE_URL = sqs.get_queue_url(QueueName=REQUEST_QUEUE)['QueueUrl']
RESPONSE_QUEUE_URL = sqs.get_queue_url(QueueName=RESPONSE_QUEUE)['QueueUrl']
pending_requests = {}
pending_requests_lock = threading.Lock()

app = Flask(__name__)

def retrieve_responses():
    while True:
        try:
            resp_queue_msg = sqs.receive_message(
                QueueUrl=RESPONSE_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5
            )
            
            if 'Messages' in resp_queue_msg :
                for message in resp_queue_msg ['Messages']:
                    receipt_handle = message['ReceiptHandle']
                    basename  = json.loads(message['Body']).get('filename')
                    result = json.loads(message['Body']).get('result')
                    
                    with pending_requests_lock:
                        for filename in list(pending_requests.keys()):
                            if filename.startswith(basename + '.') or filename == basename:
                                formatted_result = f"{basename}:{result}"
                                pending_requests[filename] = formatted_result
                                break
                    
                    sqs.delete_message(
                        QueueUrl=RESPONSE_QUEUE_URL,
                        ReceiptHandle=receipt_handle
                    )
            else:
                print("No new messages in response queue.")
        except Exception as e:
            time.sleep(5)

response_thread = threading.Thread(target=retrieve_responses, daemon=True)
response_thread.start()

@app.route('/', methods=['POST'])
def process_post_request():
    try:    
        file = request.files['inputFile']
        filename = file.filename
        request_id = str(uuid.uuid4())
        
        s3.upload_fileobj(file, INPUT_BUCKET, filename)
        
        message = {
            'request_id': request_id,
            'filename': filename
        }
        
        with pending_requests_lock:
            pending_requests[filename] = None
        
        sqs.send_message(
            QueueUrl=REQUEST_QUEUE_URL,
            MessageBody=json.dumps(message)
        )

        start_time = time.time()
        wait_until = 60 
        while time.time() - start_time < wait_until:
            with pending_requests_lock:
                if pending_requests.get(filename) is not None:
                    result = pending_requests.pop(filename)
                    return Response(result, status=200, mimetype='text/plain')
            time.sleep(0.1)

        return Response("Request timed out", status=504)
    
    except Exception as e:
        return Response(f"Error: {str(e)}", status=500)

if __name__ == '__main__':
    print("Starting web server on port 8000...")
    app.run(host='0.0.0.0', port=8000)
