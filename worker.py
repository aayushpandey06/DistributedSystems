import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import threading
import random
from datetime import datetime
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
MASTER_URL = 'http://localhost:5000'
WORKER_ID = None
WORKER_NAME = f"Worker-{random.randint(1000, 9999)}"

# Configure HTTP session with retry
session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

def register_worker():
    global WORKER_ID
    try:
        response = session.post(
            f'{MASTER_URL}/register',
            json={'name': WORKER_NAME},
            timeout=10
        )
        if response.status_code == 200:
            WORKER_ID = response.json()['worker_id']
            logger.info(f"Registered as Worker ID: {WORKER_ID}")
            return True
        else:
            logger.error(f"Registration failed: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        return False

def send_heartbeat():
    while True:
        try:
            response = session.post(
                f'{MASTER_URL}/heartbeat/{WORKER_ID}',
                json={'name': WORKER_NAME},
                timeout=5
            )
            if response.status_code != 200:
                logger.warning(f"Heartbeat failed: {response.text}")
        except Exception as e:
            logger.error(f"Heartbeat error: {str(e)}")
        
        time.sleep(3)

def process_task(task):
    task_id = task['id']
    logger.info(f"Starting task {task_id}: {task['description']}")
    
    # Acknowledge task first
    try:
        ack_response = session.post(
            f'{MASTER_URL}/acknowledge_task/{task_id}',
            json={'worker_id': WORKER_ID},
            timeout=5
        )
        if ack_response.status_code != 200:
            logger.error(f"Failed to acknowledge task {task_id}")
            return
    except Exception as e:
        logger.error(f"Acknowledge failed: {str(e)}")
        return
    
    # Simulate work with random processing time
    processing_time = random.randint(1, 10)
    for i in range(processing_time):
        time.sleep(1)
        logger.info(f"Processing task {task_id} ({i+1}/{processing_time})")
    
    # Random chance of failure (10%)
    if random.random() < 0.1:
        logger.error(f"Simulated failure processing task {task_id}")
        return
    
    # Complete the task
    result = f"Successfully processed by {WORKER_NAME} in {processing_time}s"
    try:
        response = session.post(
            f'{MASTER_URL}/task/complete',
            json={
                'task_id': task_id,
                'worker_id': WORKER_ID,
                'result': result
            },
            timeout=10
        )
        if response.status_code == 200:
            logger.info(f"Completed task {task_id}")
        else:
            logger.error(f"Completion failed: {response.text}")
    except Exception as e:
        logger.error(f"Completion error: {str(e)}")

def fetch_and_process_tasks():
    backoff = 1  # Initial backoff time in seconds
    max_backoff = 30  # Maximum backoff time
    
    while True:
        try:
            response = session.get(
                f'{MASTER_URL}/get_task',
                timeout=10
            )
            
            if response.status_code == 200:
                task = response.json()
                if 'id' in task:
                    # Mark as processing first
                    session.post(
                        f'{MASTER_URL}/task/complete',
                        json={
                            'task_id': task['id'],
                            'worker_id': WORKER_ID,
                            'result': None
                        },
                        timeout=5
                    )
                    process_task(task)
                    backoff = 1  # Reset backoff on success
                else:
                    logger.info("No tasks available")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
            elif response.status_code == 404:
                logger.info("No tasks in queue")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            else:
                logger.error(f"Unexpected response: {response.status_code}")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
        
        except Exception as e:
            logger.error(f"Fetch error: {str(e)}")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

if __name__ == '__main__':
    logger.info(f"Starting worker {WORKER_NAME}")
    
    # Attempt registration with retries
    registration_attempts = 0
    max_attempts = 5
    
    while not register_worker() and registration_attempts < max_attempts:
        registration_attempts += 1
        wait_time = min(2 ** registration_attempts, 30)  # Exponential backoff
        logger.info(f"Retrying registration in {wait_time} seconds...")
        time.sleep(wait_time)
    
    if not WORKER_ID:
        logger.error("Failed to register worker after multiple attempts. Exiting.")
        sys.exit(1)
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat)
    heartbeat_thread.daemon = True
    heartbeat_thread.start()
    
    # Start task processing
    fetch_and_process_tasks()