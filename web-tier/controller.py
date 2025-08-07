import boto3
import time
import logging
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class AutoscalingController:
    def __init__(self, id=None, region=None):
        self.id = id or os.getenv('ID')
        self.region = region or os.getenv('AWS_REGION', 'us-east-1')
        
        if not self.id:
            raise ValueError("ID environment variable is required")
        
        self.request_queue = f"{self.id}-req-queue"
        self.response_queue = f"{self.id}-resp-queue"
        self.app_instance_name = "app-tier-instance"
        self.max_instances = int(os.getenv('MAX_INSTANCES', '15'))
        self.check_interval = int(os.getenv('CHECK_INTERVAL', '5'))
        self.circuit_breaker_failures = 0
        self.circuit_breaker_threshold = 5
        self.circuit_breaker_timeout = 60
        self.last_circuit_breaker_failure = 0
        
        # Initialize AWS clients with error handling
        try:
            self.ec2_client = boto3.client('ec2', region_name=self.region)
            self.sqs_client = boto3.client('sqs', region_name=self.region)
            self.req_queue_url = self.sqs_client.get_queue_url(QueueName=self.request_queue)['QueueUrl']
            self.resp_queue_url = self.sqs_client.get_queue_url(QueueName=self.response_queue)['QueueUrl']
            logger.info("AutoscalingController initialized successfully")
        except ClientError as e:
            logger.error(f"Failed to initialize AWS clients: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during initialization: {e}")
            raise
    
    
    def _is_circuit_breaker_open(self):
        """Check if circuit breaker is open."""
        if self.circuit_breaker_failures >= self.circuit_breaker_threshold:
            if time.time() - self.last_circuit_breaker_failure < self.circuit_breaker_timeout:
                return True
            else:
                # Reset circuit breaker after timeout
                self.circuit_breaker_failures = 0
        return False
    
    def _record_failure(self):
        """Record a failure for circuit breaker."""
        self.circuit_breaker_failures += 1
        self.last_circuit_breaker_failure = time.time()
        logger.warning(f"Circuit breaker failure recorded ({self.circuit_breaker_failures}/{self.circuit_breaker_threshold})")
    
    def _record_success(self):
        """Record a success for circuit breaker."""
        if self.circuit_breaker_failures > 0:
            self.circuit_breaker_failures = 0
            logger.info("Circuit breaker reset after successful operation")
   
    def _count_running_instances(self):
        try:
            response = self.ec2_client.describe_instances(
                Filters=[
                    {'Name': 'tag:Name', 'Values': [f"{self.app_instance_name}-*"]},
                    {'Name': 'instance-state-name', 'Values': ['pending', 'running']}
                ]
            )
            
            running = []
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    instance_name = next(
                        (tag['Value'] for tag in instance.get('Tags', []) if tag['Key'] == 'Name'),
                        None
                    )
                    running.append({
                        'id': instance['InstanceId'],
                        'state': instance['State']['Name'],
                        'name': instance_name
                    })
            
            return running
            
        except ClientError as error:
            logger.error(f"Couldn't get running instances: {error}")
            return []
    
    def _find_stopped_instances(self):
        """Find stopped instances with error handling."""
        try:
            response = self.ec2_client.describe_instances(
                Filters=[
                    {'Name': 'tag:Name', 'Values': [f"{self.app_instance_name}-*"]},
                    {'Name': 'instance-state-name', 'Values': ['stopped']}
                ]
            )
            
            stopped = []
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    instance_name = next(
                        (tag['Value'] for tag in instance.get('Tags', []) if tag['Key'] == 'Name'),
                        None
                    )
                    stopped.append({
                        'id': instance['InstanceId'],
                        'name': instance_name
                    })
            
            return stopped
            
        except ClientError as e:
            logger.error(f"Error finding stopped instances: {e}")
            self._record_failure()
            return []
        except Exception as e:
            logger.error(f"Unexpected error finding stopped instances: {e}")
            self._record_failure()
            return []
    
    def _check_queue_length(self, queue_url):
        """Check queue length with error handling and retries."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                queue_info = self.sqs_client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
                )
                waiting = int(queue_info['Attributes']['ApproximateNumberOfMessages'])
                processing = int(queue_info['Attributes']['ApproximateNumberOfMessagesNotVisible'])
                return waiting + processing
                
            except ClientError as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to check queue length after {max_retries} attempts: {e}")
                    self._record_failure()
                    return 0
                logger.warning(f"Queue length check attempt {attempt + 1} failed: {e}")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Unexpected error checking queue length: {e}")
                self._record_failure()
                return 0
            
    
    def _launch_instance(self, instance_id):
        """Launch an instance with error handling."""
        try:
            self.ec2_client.start_instances(InstanceIds=[instance_id])
            logger.info(f"Started instance {instance_id}")
            self._record_success()
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'IncorrectInstanceState':
                logger.warning(f"Instance {instance_id} is already starting or running")
                return True  # Consider this a success
            elif error_code == 'InvalidInstanceID.NotFound':
                logger.error(f"Instance {instance_id} not found")
                return False
            else:
                logger.error(f"Failed to start instance {instance_id}: {e}")
                self._record_failure()
                return False
        except Exception as e:
            logger.error(f"Unexpected error starting instance {instance_id}: {e}")
            self._record_failure()
            return False
    
    def _shutdown_instance(self, instance_id):
        """Shutdown an instance with error handling."""
        try:
            self.ec2_client.stop_instances(
                InstanceIds=[instance_id],
                Force=True
            )
            logger.info(f"Shut down instance {instance_id}")
            self._record_success()
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'IncorrectInstanceState':
                logger.warning(f"Instance {instance_id} is already stopping or stopped")
                return True  # Consider this a success
            elif error_code == 'InvalidInstanceID.NotFound':
                logger.error(f"Instance {instance_id} not found")
                return False
            else:
                logger.error(f"Failed to stop instance {instance_id}: {e}")
                self._record_failure()
                return False
        except Exception as e:
            logger.error(f"Unexpected error stopping instance {instance_id}: {e}")
            self._record_failure()
            return False
    
    def start_monitoring(self):
        """Start monitoring with improved error handling and circuit breaker."""
        logger.info("Starting up the autoscaling controller")
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while True:
            try:
                # Check circuit breaker
                if self._is_circuit_breaker_open():
                    logger.warning("Circuit breaker is open, skipping scaling operations")
                    time.sleep(self.check_interval * 2)  # Wait longer when circuit breaker is open
                    continue
                
                req_count = self._check_queue_length(self.req_queue_url)
                resp_count = self._check_queue_length(self.resp_queue_url)
                total_pending = req_count + resp_count
                active_instances = self._count_running_instances()
                idle_instances = self._find_stopped_instances()
                
                logger.info(
                    f"Status: {len(active_instances)} instances running, "
                    f"{req_count} requests waiting, {resp_count} responses pending, "
                    f"Circuit breaker failures: {self.circuit_breaker_failures}"
                )
                
                # Scale up logic
                if req_count > 0 and len(active_instances) < min(req_count, self.max_instances):
                    needed = min(req_count - len(active_instances), self.max_instances - len(active_instances))
                    available = min(needed, len(idle_instances))
                    
                    successful_launches = 0
                    for i in range(available):
                        if self._launch_instance(idle_instances[i]['id']):
                            successful_launches += 1
                        else:
                            logger.warning(f"Failed to launch instance {idle_instances[i]['id']}")
                    
                    if successful_launches > 0:
                        logger.info(f"Successfully launched {successful_launches}/{available} instances")

                # Scale down logic
                elif total_pending == 0 and len(active_instances) > 0:
                    time.sleep(1)  # Brief pause before double-checking
                    req_count = self._check_queue_length(self.req_queue_url)
                    resp_count = self._check_queue_length(self.resp_queue_url)
                    
                    if req_count + resp_count == 0:
                        successful_shutdowns = 0
                        for instance in active_instances:
                            if self._shutdown_instance(instance['id']):
                                successful_shutdowns += 1
                            else:
                                logger.warning(f"Failed to shutdown instance {instance['id']}")
                        
                        if successful_shutdowns > 0:
                            logger.info(f"Successfully shut down {successful_shutdowns}/{len(active_instances)} instances")
                
                # Reset consecutive error counter on successful iteration
                consecutive_errors = 0
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error in monitoring loop (consecutive errors: {consecutive_errors}): {e}")
                self._record_failure()
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.critical(f"Too many consecutive errors ({consecutive_errors}), shutting down controller")
                    raise
                
                # Exponential backoff on errors
                error_sleep = min(self.check_interval * (2 ** min(consecutive_errors - 1, 5)), 60)
                time.sleep(error_sleep)
                continue
            
            time.sleep(self.check_interval)
                

if __name__ == "__main__":
    try:
        controller = AutoscalingController()
        controller.start_monitoring()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down gracefully")
    except Exception as e:
        logger.critical(f"Fatal error in autoscaling controller: {e}")
        raise