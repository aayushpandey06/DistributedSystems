import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
MASTER_URL = 'http://localhost:5000'

# Configure HTTP session with retry
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

def submit_task(description):
    try:
        response = session.post(
            f'{MASTER_URL}/submit',
            json={'description': description},
            timeout=10
        )
        if response.status_code == 200:
            task_id = response.json()['task_id']
            logger.info(f"Task submitted with ID: {task_id}")
            return task_id
        else:
            logger.error(f"Submission failed: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Submission error: {str(e)}")
        return None

def check_status(task_id):
    try:
        response = session.get(
            f'{MASTER_URL}/status/{task_id}',
            timeout=5
        )
        if response.status_code == 200:
            task = response.json()
            print("\nTask Status:")
            print(f"ID: {task['id']}")
            print(f"Description: {task['description']}")
            print(f"Status: {task['status']}")
            if task['status'] == 'completed':
                print(f"Result: {task['result']}")
                print(f"Worker: {task['worker_id']}")
            print(f"Created: {task['created_at']}")
            if task['completed_at']:
                print(f"Completed: {task['completed_at']}")
            return True
        else:
            logger.error(f"Status check failed: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Status error: {str(e)}")
        return False

def monitor_task(task_id):
    while True:
        if check_status(task_id):
            response = session.get(
                f'{MASTER_URL}/status/{task_id}',
                timeout=5
            )
            if response.status_code == 200 and response.json()['status'] == 'completed':
                break
        time.sleep(2)

def list_workers():
    try:
        response = session.get(
            f'{MASTER_URL}/workers',
            timeout=5
        )
        if response.status_code == 200:
            workers = response.json()
            print("\nActive Workers:")
            for worker in workers:
                print(f"ID: {worker['id']}, Last Heartbeat: {worker['last_heartbeat']}")
        else:
            logger.error(f"Worker list failed: {response.text}")
    except Exception as e:
        logger.error(f"Worker list error: {str(e)}")

def main():
    print("Distributed Task Scheduler Client")
    print("=" * 40)
    
    while True:
        print("\nMenu:")
        print("1. Submit task")
        print("2. Check task status")
        print("3. Monitor task")
        print("4. List workers")
        print("5. Exit")
        
        choice = input("Enter your choice (1-5): ").strip()
        
        if choice == '1':
            description = input("Enter task description: ").strip()
            if description:
                task_id = submit_task(description)
                if task_id:
                    print("\nWould you like to monitor this task? (y/n)")
                    if input().lower() == 'y':
                        monitor_task(task_id)
            else:
                print("Description cannot be empty")
        
        elif choice == '2':
            task_id = input("Enter task ID: ").strip()
            if task_id.isdigit():
                check_status(int(task_id))
            else:
                print("Invalid task ID")
        
        elif choice == '3':
            task_id = input("Enter task ID to monitor: ").strip()
            if task_id.isdigit():
                monitor_task(int(task_id))
            else:
                print("Invalid task ID")
        
        elif choice == '4':
            list_workers()
        
        elif choice == '5':
            print("Exiting...")
            break
        
        else:
            print("Invalid choice. Please enter 1-5")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nClient terminated by user")
    except Exception as e:
        logger.error(f"Client error: {str(e)}")