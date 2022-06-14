from __future__ import absolute_import

from celery import Celery
from mtgeapp.utils.config_parser import parse_config
import os

config_all = parse_config('worker/config_celery.yml')
config = config_all.get(os.environ.get('celery_env', 'dev'))

# TODO: This shouldnt be needed and env variable should get picked by pycharm
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


def make_celery():

    # these two are needed for running on WINDOWS. One of them is related to how to launch
    if os.name == "nt":
        os.environ.setdefault('FORKED_BY_MULTIPROCESSING', '1')
       #celery worker -A celerytest.celeryworker --pool=gevent -c 4 -l debug -f celery.log

    broker_transport_options = {'queue_name_prefix': 'pmc-'}

    celery = Celery(
        'mtgeapp.worker',
         broker=config['broker'],
         backend=config['backend'],
         include=['mtgeapp.worker.tasks'],
    )

    celery.conf.update(
        result_expires=config['result_expires'],
        broker_transport_options=config['broker_transport_options'],
        task_time_limit=config['task_time_limit'],
        task_track_started=True,
        task_serializer='pickle',
        result_serializer='pickle',
        accept_content=['json', 'pickle'],
        result_accept_content=['json', 'pickle']
        # worker_concurrency=1
    )
    return celery


celery_app = make_celery()


if __name__ == '__main__':
    # worker = celery_app.Worker(
    #     include=['mtgeapp.worker.tasks'],
    # )

    # worker.start()
    celery_app.worker_main(argv=['worker', '--loglevel=info', '--concurrency=1'])
