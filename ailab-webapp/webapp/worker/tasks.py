from celery import Task
from celery.exceptions import MaxRetriesExceededError
import time
from .celery_app import celery_app


@celery_app.task(name="test_celery")
def test_celery(sleep: int) -> str:
    time.sleep(sleep)
    return 'Celery test finished'
