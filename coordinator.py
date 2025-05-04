from flask import Flask, request, jsonify
import threading
import time
import sqlite3
from datetime import datetime
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize database
def init_db():
    conn = sqlite3.connect('tasks.db', timeout=10)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tasks
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  description TEXT,
                  status TEXT,
                  result TEXT,
                  worker_id INTEGER,
                  created_at TIMESTAMP,
                  completed_at TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS workers
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  last_heartbeat TIMESTAMP,
                  status TEXT)''')
    conn.commit()
    conn.close()

init_db()

# Worker and task management
TASK_QUEUE = []
HEARTBEAT_INTERVAL = 5
WORKER_TIMEOUT = 15
workers_lock = threading.Lock()
queue_lock = threading.Lock()

@app.route('/submit', methods=['POST'])
def submit_task():
    task_data = request.json
    if not task_data or 'description' not in task_data:
        return jsonify({'error': 'Invalid task data'}), 400
    
    try:
        conn = sqlite3.connect('tasks.db', timeout=10)
        c = conn.cursor()
        c.execute('''INSERT INTO tasks (description, status, created_at)
                     VALUES (?, ?, ?)''',
                  (task_data['description'], 'pending', datetime.now()))
        task_id = c.lastrowid
        conn.commit()
        
        with queue_lock:
            TASK_QUEUE.append(task_id)
            logger.info(f"Task {task_id} added to queue")
        
        return jsonify({'task_id': task_id, 'status': 'submitted'})
    
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': 'Database operation failed'}), 500
    finally:
        conn.close()

@app.route('/status/<int:task_id>', methods=['GET'])
def get_status(task_id):
    try:
        conn = sqlite3.connect('tasks.db', timeout=10)
        c = conn.cursor()
        c.execute('SELECT * FROM tasks WHERE id = ?', (task_id,))
        task = c.fetchone()
        
        if not task:
            return jsonify({'error': 'Task not found'}), 404
        
        columns = ['id', 'description', 'status', 'result', 'worker_id', 'created_at', 'completed_at']
        task_dict = dict(zip(columns, task))
        task_dict['created_at'] = task_dict['created_at'] or None
        task_dict['completed_at'] = task_dict['completed_at'] or None
        
        return jsonify(task_dict)
    
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': 'Database operation failed'}), 500
    finally:
        conn.close()

@app.route('/register', methods=['POST'])
def register_worker():
    try:
        conn = sqlite3.connect('tasks.db', timeout=10)
        c = conn.cursor()
        
        # Get the next worker ID
        c.execute('SELECT MAX(id) FROM workers')
        max_id = c.fetchone()[0] or 0
        worker_id = max_id + 1
        
        c.execute('''INSERT INTO workers (id, last_heartbeat, status)
                     VALUES (?, ?, ?)''',
                  (worker_id, datetime.now(), 'active'))
        conn.commit()
        
        with workers_lock:
            logger.info(f"Worker {worker_id} registered")
        
        return jsonify({'worker_id': worker_id, 'status': 'registered'})
    
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': 'Registration failed'}), 500
    finally:
        conn.close()

@app.route('/heartbeat/<int:worker_id>', methods=['POST'])
def receive_heartbeat(worker_id):
    try:
        conn = sqlite3.connect('tasks.db', timeout=10)
        c = conn.cursor()
        c.execute('''UPDATE workers SET last_heartbeat = ?, status = ?
                     WHERE id = ?''',
                  (datetime.now(), 'active', worker_id))
        conn.commit()
        
        return jsonify({'status': 'heartbeat received'})
    
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': 'Heartbeat processing failed'}), 500
    finally:
        conn.close()

@app.route('/task/complete', methods=['POST'])
def complete_task():
    data = request.json
    task_id = data.get('task_id')
    worker_id = data.get('worker_id')
    result = data.get('result')
    
    if not all([task_id, worker_id]):
        return jsonify({'error': 'Missing data'}), 400
    
    try:
        conn = sqlite3.connect('tasks.db', timeout=10)
        c = conn.cursor()
        
        status = 'completed' if result else 'processing'
        c.execute('''UPDATE tasks SET status = ?, result = ?, 
                     completed_at = ?, worker_id = ?
                     WHERE id = ?''',
                  (status, result, 
                   datetime.now() if result else None, 
                   worker_id, task_id))
        conn.commit()
        
        logger.info(f"Task {task_id} updated by worker {worker_id}")
        return jsonify({'status': 'task updated'})
    
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': 'Task completion failed'}), 500
    finally:
        conn.close()

@app.route('/get_task', methods=['GET'])
def get_task():
    with queue_lock:
        if not TASK_QUEUE:
            return jsonify({'error': 'No tasks available'}), 404
        
        task_id = TASK_QUEUE[0]  # Peek without removing
    
    try:
        conn = sqlite3.connect('tasks.db', timeout=10)
        c = conn.cursor()
        c.execute('SELECT * FROM tasks WHERE id = ?', (task_id,))
        task = c.fetchone()
        
        if not task:
            return jsonify({'error': 'Task not found'}), 404
        
        columns = ['id', 'description', 'status', 'result', 'worker_id', 'created_at', 'completed_at']
        task_dict = dict(zip(columns, task))
        
        return jsonify(task_dict)
    
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({'error': 'Database operation failed'}), 500
    finally:
        conn.close()

@app.route('/acknowledge_task/<int:task_id>', methods=['POST'])
def acknowledge_task(task_id):
    worker_id = request.json.get('worker_id')
    if not worker_id:
        return jsonify({'error': 'Worker ID required'}), 400
    
    with queue_lock:
        if task_id in TASK_QUEUE:
            TASK_QUEUE.remove(task_id)
            logger.info(f"Task {task_id} acknowledged by worker {worker_id}")
            return jsonify({'status': 'acknowledged'})
    
    return jsonify({'error': 'Task not in queue'}), 404

def check_workers():
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        current_time = datetime.now()
        
        try:
            conn = sqlite3.connect('tasks.db', timeout=10)
            c = conn.cursor()
            
            # Check for failed workers
            c.execute('''SELECT id FROM workers 
                         WHERE status = 'active' 
                         AND datetime(last_heartbeat) < datetime(?, ?)''',
                      (current_time, f'-{WORKER_TIMEOUT} seconds'))
            failed_workers = c.fetchall()
            
            for (worker_id,) in failed_workers:
                # Mark worker as failed
                c.execute('''UPDATE workers SET status = 'failed'
                             WHERE id = ?''', (worker_id,))
                
                # Requeue tasks assigned to failed worker
                c.execute('''SELECT id FROM tasks 
                             WHERE worker_id = ? AND status = 'processing' ''',
                          (worker_id,))
                failed_tasks = c.fetchall()
                
                with queue_lock:
                    for (task_id,) in failed_tasks:
                        TASK_QUEUE.append(task_id)
                        c.execute('''UPDATE tasks SET status = 'pending', 
                                     worker_id = NULL WHERE id = ?''',
                                  (task_id,))
                
                logger.warning(f"Worker {worker_id} marked as failed. Requeued {len(failed_tasks)} tasks.")
            
            conn.commit()
        
        except sqlite3.Error as e:
            logger.error(f"Worker check failed: {str(e)}")
        finally:
            conn.close()

if __name__ == '__main__':
    # Start background thread for worker monitoring
    monitor_thread = threading.Thread(target=check_workers)
    monitor_thread.daemon = True
    monitor_thread.start()
    
    # Start Flask server
    app.run(host='0.0.0.0', port=5000, threaded=True)