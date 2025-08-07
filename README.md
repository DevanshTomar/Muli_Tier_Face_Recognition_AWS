# Multi-Tier Face Recognition System on AWS

A scalable face recognition system built on AWS infrastructure using a multi-tier architecture. The system processes image uploads, performs face recognition, and returns results through a web interface.

## Architecture

The system consists of three main tiers:

1. **Web Tier**
   - Flask web server handling HTTP requests
   - Accepts image uploads
   - Manages request/response flow
   - Communicates with S3 and SQS

2. **App Tier**
   - Processes face recognition requests
   - Runs on EC2 instances
   - Auto-scales based on queue length
   - Handles image processing and face recognition

3. **Controller Tier**
   - Manages EC2 instance scaling
   - Monitors SQS queue lengths
   - Launches/terminates instances as needed

## AWS Services Used

- **S3**: Storage for input and output images
- **SQS**: Message queues for request/response handling
- **EC2**: Compute instances for face recognition processing
- **IAM**: Security and access management

## Prerequisites

- Python 3.x
- AWS Account with appropriate permissions
- AWS CLI configured with credentials
- Required Python packages (see requirements.txt)

## Setup

1. **Configure AWS Credentials**
   ```bash
   aws configure
   ```
   Enter your AWS Access Key ID, Secret Access Key, and default region (us-east-1)

2. **Create Required AWS Resources**
   - S3 buckets for input/output
   - SQS queues for requests/responses
   - EC2 instances with appropriate IAM roles

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Project Structure

```
.
├── web-tier/
│   ├── server.py      # Flask web server
│   └── controller.py  # Auto-scaling controller
├── app-tier/
│   └── backend.py     # Face recognition processing
├── requirements.txt   # Python dependencies
└── README.md         # This file
```


## Enhanced Error Handling Features

### Web Tier Improvements
- **File Validation**: Checks file type, size, and content before processing
- **Retry Logic**: Automatic retries for S3 uploads and SQS operations with exponential backoff
- **Health Endpoint**: `/health` endpoint for monitoring service availability
- **Structured Logging**: Comprehensive logging with request IDs for tracking
- **Proper HTTP Status Codes**: Appropriate error responses (400, 500, 503, 504)

### App Tier Improvements
- **Dead Letter Queue**: Failed messages are sent to DLQ for manual inspection
- **Retry Mechanisms**: Configurable retries for all AWS operations
- **Face Recognition Error Handling**: Graceful handling of missing faces or corrupted images
- **Resource Cleanup**: Automatic cleanup of temporary files
- **Circuit Breaker Pattern**: Prevents cascade failures during AWS outages
- **Consecutive Failure Tracking**: Exits after too many consecutive failures

### Controller Improvements
- **Circuit Breaker**: Pauses scaling operations during AWS API failures
- **Instance State Handling**: Properly handles already running/stopped instances
- **Error Recovery**: Exponential backoff and automatic recovery from transient failures
- **Comprehensive Monitoring**: Enhanced logging with circuit breaker status
- **Graceful Shutdown**: Handles interrupt signals properly

## Auto-scaling Behavior

The system automatically scales based on queue length:
- Launches new instances when request queue length increases
- Terminates instances when queues are empty
- Maximum of 15 concurrent instances (configurable)
- 5-second check interval for scaling decisions (configurable)
- Circuit breaker protection during AWS API failures
- Tracks and reports scaling operation success rates

## Environment Variables

Create a `.env` file in the project root with the following variables:

```bash
# Required
ID=your-unique-identifier
AWS_REGION=us-east-1

# Web Tier (Optional)
REQUEST_TIMEOUT=60
MAX_FILE_SIZE=10485760

# App Tier (Optional)
MAX_RETRIES=3
RETRY_DELAY=2
VISIBILITY_TIMEOUT=300

# Controller (Optional)
MAX_INSTANCES=15
CHECK_INTERVAL=5
```



