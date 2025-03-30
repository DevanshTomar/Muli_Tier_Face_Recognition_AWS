import boto3
import time
import logging
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class AutoscalingController:
    def __init__(self, asu_id, region='us-east-1'):
        self.asu_id = asu_id
        self.region = region
        self.request_queue = f"{asu_id}-req-queue"
        self.response_queue = f"{asu_id}-resp-queue"
        self.app_instance_name = "app-tier-instance"
        self.ec2_client = boto3.client('ec2', region_name=region)
        self.sqs_client = boto3.client('sqs', region_name=region)
        self.max_instances = 15  
        self.check_interval = 5
        self.req_queue_url = self.sqs_client.get_queue_url(QueueName=self.request_queue)['QueueUrl']
        self.resp_queue_url = self.sqs_client.get_queue_url(QueueName=self.response_queue)['QueueUrl']
    
   
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
    
    def _check_queue_length(self, queue_url):
        queue_info = self.sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        waiting = int(queue_info['Attributes']['ApproximateNumberOfMessages'])
        processing = int(queue_info['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        return waiting + processing
            
    
    def _launch_instance(self, instance_id):
        self.ec2_client.start_instances(InstanceIds=[instance_id])
        logger.info(f"Started instance {instance_id}")
        return True
    
    def _shutdown_instance(self, instance_id):
        self.ec2_client.stop_instances(
            InstanceIds=[instance_id],
            Force=True
        )
        logger.info(f"Shut down instance {instance_id}")
        return True
    
    def start_monitoring(self):
        logger.info("Starting up the autoscaling controller")
        while True:
            req_count = self._check_queue_length(self.req_queue_url)
            resp_count = self._check_queue_length(self.resp_queue_url)
            total_pending = req_count + resp_count
            active_instances = self._count_running_instances()
            idle_instances = self._find_stopped_instances()
            
            logger.info(
                f"Status: {len(active_instances)} instances running, "
                f"{req_count} requests waiting, {resp_count} responses pending"
            )
            
            if req_count > 0 and len(active_instances) < min(req_count, self.max_instances):
                needed = min(req_count - len(active_instances), self.max_instances - len(active_instances))
                available = min(needed, len(idle_instances))
                for i in range(available):
                    self._launch_instance(idle_instances[i]['id'])

            elif total_pending == 0 and len(active_instances) > 0:
                time.sleep(1)  
                req_count = self._check_queue_length(self.req_queue_url)
                resp_count = self._check_queue_length(self.resp_queue_url)
                
                if req_count + resp_count == 0:
                    for instance in active_instances:
                        self._shutdown_instance(instance['id'])
            
            time.sleep(self.check_interval)
                

if __name__ == "__main__":
    asu_id = "1220103989"
    controller = AutoscalingController(asu_id)
    controller.start_monitoring()