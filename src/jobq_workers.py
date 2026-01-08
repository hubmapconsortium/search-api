import os
from flask import Flask
from atlas_consortia_jobq import JobQueue

if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    app = Flask(__name__, 
                instance_path=os.path.join(script_dir, 'instance'),
                instance_relative_config=True)
    app.config.from_pyfile('app.cfg')
    queue = JobQueue(
        redis_host=app.config.get('REDIS_HOST', 'localhost'),
        redis_port=int(app.config.get('REDIS_PORT', 6379)),
        redis_db=int(app.config.get('REDIS_DB', 0))
    )
    queue_workers = int(app.config.get('QUEUE_WORKERS', 4))
    queue.start_workers(num_workers=queue_workers)