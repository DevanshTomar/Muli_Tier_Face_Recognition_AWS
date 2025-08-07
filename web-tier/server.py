import boto3
import json
import os
import uuid
from flask import Flask, request, Response
import threading
import time
import logging
from dotenv import load_dotenv
from botocore.exceptions import ClientError, BotoCoreError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

ID = os.getenv('ID')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '60'))
MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE', '10485760'))  # 10MB default
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'bmp'}

# Validate required environment variables
if not ID:
    logger.error("ID environment variable is required")
    raise ValueError("ID environment variable is required")

INPUT_BUCKET = f"{ID}-in-bucket"
OUTPUT_BUCKET = f"{ID}-out-bucket"
REQUEST_QUEUE = f"{ID}-req-queue"
RESPONSE_QUEUE = f"{ID}-resp-queue"

# Initialize AWS clients with error handling
try:
    s3 = boto3.client('s3', region_name=AWS_REGION)
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    REQUEST_QUEUE_URL = sqs.get_queue_url(QueueName=REQUEST_QUEUE)['QueueUrl']
    RESPONSE_QUEUE_URL = sqs.get_queue_url(QueueName=RESPONSE_QUEUE)['QueueUrl']
    logger.info("AWS clients initialized successfully")
except ClientError as e:
    logger.error(f"Failed to initialize AWS clients: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error during AWS client initialization: {e}")
    raise

pending_requests = {}
pending_requests_lock = threading.Lock()

app = Flask(__name__)

def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def validate_file(file):
    """Validate uploaded file."""
    if not file or not file.filename:
        return False, "No file provided"
    
    if not allowed_file(file.filename):
        return False, f"File type not allowed. Allowed types: {', '.join(ALLOWED_EXTENSIONS)}"
    
    # Check file size
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)  # Reset file pointer
    
    if file_size > MAX_FILE_SIZE:
        return False, f"File too large. Maximum size: {MAX_FILE_SIZE / 1024 / 1024:.1f}MB"
    
    if file_size == 0:
        return False, "Empty file not allowed"
    
    return True, "Valid file"

def retrieve_responses():
    """Background thread to retrieve responses from SQS with improved error handling."""
    retry_count = 0
    max_retries = 3
    base_delay = 1
    
    while True:
        try:
            resp_queue_msg = sqs.receive_message(
                QueueUrl=RESPONSE_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5
            )
            
            if 'Messages' in resp_queue_msg:
                for message in resp_queue_msg['Messages']:
                    try:
                        receipt_handle = message['ReceiptHandle']
                        message_body = json.loads(message['Body'])
                        basename = message_body.get('filename')
                        result = message_body.get('result')
                        
                        if not basename or result is None:
                            logger.warning(f"Invalid message format: {message_body}")
                            continue
                        
                        with pending_requests_lock:
                            for filename in list(pending_requests.keys()):
                                if filename.startswith(basename + '.') or filename == basename:
                                    formatted_result = f"{basename}:{result}"
                                    pending_requests[filename] = formatted_result
                                    logger.info(f"Result received for {filename}")
                                    break
                        
                        # Delete message only after successful processing
                        sqs.delete_message(
                            QueueUrl=RESPONSE_QUEUE_URL,
                            ReceiptHandle=receipt_handle
                        )
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse message body: {e}")
                    except ClientError as e:
                        logger.error(f"Failed to delete message: {e}")
                    except Exception as e:
                        logger.error(f"Unexpected error processing message: {e}")
            
            # Reset retry count on success
            retry_count = 0
            
        except ClientError as e:
            retry_count += 1
            delay = base_delay * (2 ** min(retry_count - 1, 5))  # Exponential backoff
            logger.error(f"AWS error in retrieve_responses (attempt {retry_count}): {e}")
            
            if retry_count >= max_retries:
                logger.error("Max retries exceeded for SQS operations, resetting counter")
                retry_count = 0
                delay = 30  # Longer delay before trying again
            
            time.sleep(delay)
            
        except Exception as e:
            logger.error(f"Unexpected error in retrieve_responses: {e}")
            time.sleep(5)

response_thread = threading.Thread(target=retrieve_responses, daemon=True)
response_thread.start()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        # Test AWS connectivity
        sqs.get_queue_attributes(QueueUrl=REQUEST_QUEUE_URL, AttributeNames=['ApproximateNumberOfMessages'])
        return Response("OK", status=200)
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return Response("Service Unavailable", status=503)

@app.route('/', methods=['POST'])
def process_post_request():
    request_id = str(uuid.uuid4())
    logger.info(f"Processing request {request_id}")
    
    try:
        # Validate request
        if 'inputFile' not in request.files:
            return Response("No inputFile provided", status=400)
        
        file = request.files['inputFile']
        valid, message = validate_file(file)
        if not valid:
            logger.warning(f"File validation failed for {request_id}: {message}")
            return Response(f"File validation error: {message}", status=400)
        
        filename = file.filename
        
        # Upload to S3 with retry logic
        max_upload_retries = 3
        for attempt in range(max_upload_retries):
            try:
                file.seek(0)  # Reset file pointer
                s3.upload_fileobj(file, INPUT_BUCKET, filename)
                logger.info(f"File {filename} uploaded to S3 successfully")
                break
            except ClientError as e:
                if attempt == max_upload_retries - 1:
                    logger.error(f"Failed to upload {filename} after {max_upload_retries} attempts: {e}")
                    return Response("Failed to upload file to storage", status=500)
                logger.warning(f"Upload attempt {attempt + 1} failed: {e}, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        # Prepare message
        message = {
            'request_id': request_id,
            'filename': filename
        }
        
        # Add to pending requests
        with pending_requests_lock:
            pending_requests[filename] = None
        
        # Send message to SQS with retry logic
        max_sqs_retries = 3
        for attempt in range(max_sqs_retries):
            try:
                sqs.send_message(
                    QueueUrl=REQUEST_QUEUE_URL,
                    MessageBody=json.dumps(message)
                )
                logger.info(f"Message sent to SQS for {filename}")
                break
            except ClientError as e:
                if attempt == max_sqs_retries - 1:
                    # Cleanup: remove from pending requests and delete uploaded file
                    with pending_requests_lock:
                        pending_requests.pop(filename, None)
                    try:
                        s3.delete_object(Bucket=INPUT_BUCKET, Key=filename)
                    except Exception:
                        pass  # Best effort cleanup
                    logger.error(f"Failed to send message after {max_sqs_retries} attempts: {e}")
                    return Response("Failed to queue request for processing", status=500)
                logger.warning(f"SQS attempt {attempt + 1} failed: {e}, retrying...")
                time.sleep(2 ** attempt)

        # Wait for result with timeout
        start_time = time.time()
        while time.time() - start_time < REQUEST_TIMEOUT:
            with pending_requests_lock:
                if pending_requests.get(filename) is not None:
                    result = pending_requests.pop(filename)
                    logger.info(f"Request {request_id} completed successfully")
                    return Response(result, status=200, mimetype='text/plain')
            time.sleep(0.1)

        # Cleanup on timeout
        with pending_requests_lock:
            pending_requests.pop(filename, None)
        
        logger.warning(f"Request {request_id} timed out after {REQUEST_TIMEOUT} seconds")
        return Response("Request timed out", status=504)
    
    except ClientError as e:
        logger.error(f"AWS error processing request {request_id}: {e}")
        return Response("Service temporarily unavailable", status=503)
    except Exception as e:
        logger.error(f"Unexpected error processing request {request_id}: {e}")
        return Response("Internal server error", status=500)

@app.errorhandler(413)
def file_too_large(error):
    return Response("File too large", status=413)

@app.errorhandler(404)
def not_found(error):
    return Response("Endpoint not found", status=404)

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return Response("Internal server error", status=500)

if __name__ == '__main__':
    logger.info("Starting web server on port 8000...")
    app.run(host='0.0.0.0', port=8000, debug=False)
