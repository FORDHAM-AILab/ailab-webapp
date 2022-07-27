from multiprocessing import cpu_count
import os

working_dir = os.path.abspath(os.getcwd())

# Socket Path
bind = os.path.join(working_dir, 'gunicorn.sock')

# Worker Options
workers = cpu_count() + 1
worker_class = 'uvicorn.workers.UvicornWorker'

# Logging Options
loglevel = 'debug'
accesslog = os.path.join(working_dir,'access_log')
errorlog = os.path.join(working_dir,'error_log')