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


## Auto-scaling Behavior

The system automatically scales based on queue length:
- Launches new instances when request queue length increases
- Terminates instances when queues are empty
- Maximum of 15 concurrent instances
- 5-second check interval for scaling decisions



