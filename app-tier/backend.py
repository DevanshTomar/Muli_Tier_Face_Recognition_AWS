import boto3
import json
import os
import sys
import time
import logging
import traceback
from face_recognition import face_match
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
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '2'))
VISIBILITY_TIMEOUT = int(os.getenv('VISIBILITY_TIMEOUT', '300'))

# Validate required environment variables
if not ID:
    logger.error("ID environment variable is required")
    sys.exit(1)

# AWS resource names and clients
INPUT_BUCKET = f"{ID}-in-bucket"
OUTPUT_BUCKET = f"{ID}-out-bucket"
REQUEST_QUEUE = f"{ID}-req-queue"
RESPONSE_QUEUE = f"{ID}-resp-queue"
DLQ_QUEUE = f"{ID}-dlq-queue"  # Dead Letter Queue

try:
    s3 = boto3.client('s3', region_name=AWS_REGION)
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    REQUEST_QUEUE_URL = sqs.get_queue_url(QueueName=REQUEST_QUEUE)['QueueUrl']
    RESPONSE_QUEUE_URL = sqs.get_queue_url(QueueName=RESPONSE_QUEUE)['QueueUrl']
    
    # Try to get DLQ URL, create if doesn't exist
    try:
        DLQ_QUEUE_URL = sqs.get_queue_url(QueueName=DLQ_QUEUE)['QueueUrl']
    except ClientError:
        logger.warning(f"DLQ {DLQ_QUEUE} not found, proceeding without DLQ")
        DLQ_QUEUE_URL = None
    
    logger.info("AWS clients initialized successfully")
except ClientError as e:
    logger.error(f"Failed to initialize AWS clients: {e}")
    sys.exit(1)

def validate_face_recognition_data():
    """Validate that face recognition data file exists."""
    if not os.path.exists('data.pt'):
        logger.error("Face recognition data file 'data.pt' not found")
        return False
    return True

def send_to_dlq(message, error_reason):
    """Send failed message to Dead Letter Queue."""
    if not DLQ_QUEUE_URL:
        logger.warning("No DLQ configured, cannot send failed message")
        return False
    
    try:
        dlq_message = {
            'original_message': message,
            'error_reason': error_reason,
            'timestamp': time.time(),
            'failure_count': message.get('failure_count', 0) + 1
        }
        
        sqs.send_message(
            QueueUrl=DLQ_QUEUE_URL,
            MessageBody=json.dumps(dlq_message)
        )
        logger.info(f"Message sent to DLQ: {error_reason}")
        return True
    except ClientError as e:
        logger.error(f"Failed to send message to DLQ: {e}")
        return False

def download_file_with_retry(bucket, key, local_path, max_retries=MAX_RETRIES):
    """Download file from S3 with retry logic."""
    for attempt in range(max_retries):
        try:
            s3.download_file(bucket, key, local_path)
            logger.info(f"Successfully downloaded {key} from {bucket}")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.error(f"File {key} not found in bucket {bucket}")
                return False
            elif error_code == 'NoSuchBucket':
                logger.error(f"Bucket {bucket} not found")
                return False
            else:
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to download {key} after {max_retries} attempts")
                    return False
                time.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
        except Exception as e:
            logger.error(f"Unexpected error downloading {key}: {e}")
            return False
    return False

def upload_result_with_retry(bucket, key, content, max_retries=MAX_RETRIES):
    """Upload result to S3 with retry logic."""
    for attempt in range(max_retries):
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=content)
            logger.info(f"Successfully uploaded result to {bucket}/{key}")
            return True
        except ClientError as e:
            logger.warning(f"Upload attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to upload result after {max_retries} attempts")
                return False
            time.sleep(RETRY_DELAY * (2 ** attempt))
        except Exception as e:
            logger.error(f"Unexpected error uploading result: {e}")
            return False
    return False

def send_response_with_retry(message, max_retries=MAX_RETRIES):
    """Send response to SQS with retry logic."""
    for attempt in range(max_retries):
        try:
            sqs.send_message(
                QueueUrl=RESPONSE_QUEUE_URL,
                MessageBody=json.dumps(message)
            )
            logger.info(f"Successfully sent response for {message.get('filename')}")
            return True
        except ClientError as e:
            logger.warning(f"Response send attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to send response after {max_retries} attempts")
                return False
            time.sleep(RETRY_DELAY * (2 ** attempt))
        except Exception as e:
            logger.error(f"Unexpected error sending response: {e}")
            return False
    return False

def handle_request(request):
    """Handle face recognition request with comprehensive error handling."""
    request_id = None
    file_name = None
    tmp_path = None
    
    try:
        # Parse message
        message_body = json.loads(request['Body'])
        request_id = message_body.get('request_id')
        file_name = message_body.get('filename')
        
        if not file_name:
            error_msg = "Missing filename in request"
            logger.error(error_msg)
            send_to_dlq(message_body, error_msg)
            return False
        
        logger.info(f"Processing request {request_id} for file {file_name}")
        
        match_image = file_name.split('.')[0]
        tmp_path = os.path.join('/tmp', file_name)
        
        # Download file from S3
        if not download_file_with_retry(INPUT_BUCKET, file_name, tmp_path):
            error_msg = f"Failed to download file {file_name}"
            send_to_dlq(message_body, error_msg)
            return False
        
        # Validate downloaded file
        if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) == 0:
            error_msg = f"Downloaded file {file_name} is empty or corrupted"
            logger.error(error_msg)
            send_to_dlq(message_body, error_msg)
            return False
        
        # Perform face recognition
        try:
            match_result = face_match(tmp_path, 'data.pt')[0]
            logger.info(f"Face recognition completed for {file_name}: {match_result}")
        except FileNotFoundError:
            error_msg = "Face recognition data file 'data.pt' not found"
            logger.error(error_msg)
            send_to_dlq(message_body, error_msg)
            return False
        except IndexError:
            error_msg = f"No face detection result returned for {file_name}"
            logger.error(error_msg)
            match_result = "No face detected"
        except Exception as e:
            error_msg = f"Face recognition failed for {file_name}: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            send_to_dlq(message_body, error_msg)
            return False
        
        # Upload result to S3
        if not upload_result_with_retry(OUTPUT_BUCKET, match_image, match_result):
            error_msg = f"Failed to upload result for {file_name}"
            send_to_dlq(message_body, error_msg)
            return False
        
        # Send response
        response_queue_message = {
            'filename': match_image,
            'result': match_result,
            'request_id': request_id,
            'processed_at': time.time()
        }
        
        if not send_response_with_retry(response_queue_message):
            error_msg = f"Failed to send response for {file_name}"
            logger.error(error_msg)
            return False
        
        # Delete message from request queue only after successful processing
        try:
            sqs.delete_message(
                QueueUrl=REQUEST_QUEUE_URL,
                ReceiptHandle=request['ReceiptHandle']
            )
            logger.info(f"Successfully processed and deleted message for {file_name}")
        except ClientError as e:
            logger.error(f"Failed to delete message from queue: {e}")
            # Don't return False here as the processing was successful
        
        return True
        
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON in message: {e}"
        logger.error(error_msg)
        return False
    except Exception as e:
        error_msg = f"Unexpected error processing request {request_id}: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        if file_name:
            send_to_dlq({'request_id': request_id, 'filename': file_name}, error_msg)
        return False
    finally:
        # Cleanup temporary file
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
                logger.debug(f"Cleaned up temporary file {tmp_path}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temporary file {tmp_path}: {e}")

def check_for_more_requests():
    """Check if there are more requests in the queue."""
    try:
        req_message = sqs.receive_message(
            QueueUrl=REQUEST_QUEUE_URL,
            MaxNumberOfMessages=1,
            VisibilityTimeout=0,  
            WaitTimeSeconds=1     
        )
        return 'Messages' in req_message and len(req_message['Messages']) > 0
    except ClientError as e:
        logger.error(f"Error checking for more requests: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking queue: {e}")
        return False

def main():
    """Main processing loop with improved error handling."""
    logger.info("Starting face recognition worker")
    
    # Validate face recognition data
    if not validate_face_recognition_data():
        logger.error("Face recognition data validation failed, exiting")
        sys.exit(1)
    
    consecutive_failures = 0
    max_consecutive_failures = 5
    
    while True:
        request_processed = False
        try:
            req_message = sqs.receive_message(
                QueueUrl=REQUEST_QUEUE_URL,
                MaxNumberOfMessages=1,
                VisibilityTimeout=VISIBILITY_TIMEOUT,
                WaitTimeSeconds=20  
            )

            if 'Messages' in req_message:
                request_processed = handle_request(req_message['Messages'][0])
                
                if request_processed:
                    consecutive_failures = 0  # Reset failure counter on success
                else:
                    consecutive_failures += 1
                    logger.warning(f"Request processing failed (consecutive failures: {consecutive_failures})")
            else:
                logger.info("No messages in queue, exiting")
                break
                
        except ClientError as e:
            consecutive_failures += 1
            logger.error(f"AWS error in main loop (consecutive failures: {consecutive_failures}): {e}")
        except Exception as e:
            consecutive_failures += 1
            logger.error(f"Unexpected error in main loop (consecutive failures: {consecutive_failures}): {e}")
            logger.error(traceback.format_exc())
        
        # Exit if too many consecutive failures
        if consecutive_failures >= max_consecutive_failures:
            logger.error(f"Too many consecutive failures ({consecutive_failures}), exiting")
            sys.exit(1)
        
        if request_processed:
            # Brief pause before checking for more work
            time.sleep(1)
            if check_for_more_requests():
                continue  # Process more requests
            else:
                logger.info("No more requests, exiting")
                break
        else:
            # Longer pause after failure
            time.sleep(5)
    
    logger.info("Face recognition worker shutting down")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down gracefully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
