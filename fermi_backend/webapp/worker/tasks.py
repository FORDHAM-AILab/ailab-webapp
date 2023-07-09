from typing import Callable

from celery import Task
import celery
from celery.exceptions import MaxRetriesExceededError
import time
from .celery_app import celery_app
from celery.utils.log import get_task_logger
from celery import shared_task

celery_log = get_task_logger(__name__)


@celery_app.task(name="test_celery")
def test_celery(sleep: int) -> str:
    time.sleep(sleep)
    celery_log.info(f"Order Complete!")
    return 'Celery test finished'

#
# class MLJob(celery.Task):
#     abstract = True
#
#     def on_success(self, retval, task_id, args, kwargs):
#         Job.update_job_status(scheduler_id=task_id, status=JobStatus.SUCCEEDED)
#
#     def on_failure(self, exc, task_id, args, kwargs, einfo):
#         Job.set_job_failed(scheduler_id=task_id, result=einfo.exception)
#
#
# def despatch_job(
#     func: Callable, user: int, fargs: tuple = (), fkwargs: dict = {}
# ) -> Job:
#     job = Job.create_job(
#         user_id=user.id,
#         name="adaf",
#         status=JobStatus.PENDING
#     )
#     if job is not None:
#         fkwargs["job_id"] = job.id
#         job_handler = func.apply_async(args=fargs, kwargs=fkwargs)
#         if job.update_scheduler_id(job_handler.id):
#             return job
#         else:
#             return None
#     else:
#         return None
#
#
# @celery.task(bind=True, base=MLJob)
# def run_prediction(self, arg1: str, arg2: str, job_id: str = None):
#     runs = random.randrange(1, 5)
#     start = time.time()
#     Job.update_job_timing(start_time=datetime.utcnow(), job_id=job_id)
#     for i in range(runs):
#         Job.update_job_status(job_id=job_id, status=JobStatus.IN_PROGRESS)
#         if i == 2:
#             raise ValueError("Some weird error")
#         self.update_state(
#             state="IN_PROGRESS",
#             meta={
#                 "current": i,
#                 "total": runs,
#                 "message": "Still running... {}, {}".format(arg1, arg2)
#             }
#         )
#         time.sleep(1)
#     end = time.time()
#     Job.update_job_timing(end_time=datetime.utcnow(), job_id=job_id)
#     Job.update_job_duration(job_id=job_id, duration=(end - start))
#     return {
#         "current": i,
#         "total": runs,
#         "message": "Job completed!",
#         "result": 4636
#     }
#
#
# @shared_task
# def batch_users_prediction_task(users_ids=None, start_page=0, offset=50, max_pages=1000):
#     model = ml_utils.load_model()
#     Suggestion = apps.get_model('suggestions', 'Suggestion')
#     ctype = ContentType.objects.get(app_label='movies', model='movie')
#     end_page = start_page + offset
#     if users_ids is None:
#         users_ids = profile_utils.get_recent_users()
#     movie_ids = Movie.objects.all().popular().values_list('id', flat=True)[start_page:end_page]
#     recently_suggested = Suggestion.objects.get_recently_suggested(movie_ids, users_ids)
#     new_suggestion = []
#     if not movie_ids.exists():
#         return
#     for movie_id in movie_ids:
#         users_done = recently_suggested.get(f"{movie_id}") or []
#         for u in users_ids:
#             if u in users_done:
#                 # print(movie_id, 'is done for', u, 'user')
#                 continue
#             if u is None:
#                 continue
#             if movie_id is None:
#                 continue
#             pred = model.predict(uid=u, iid=movie_id).est
#             data = {
#                 'user_id': u,
#                 'object_id': movie_id,
#                 'value': pred,
#                 'content_type': ctype
#             }
#             try:
#                 obj, _ = Suggestion.objects.get_or_create(user_id=u, object_id=movie_id, content_type=ctype)
#             except Suggestion.MultipleObjectsReturned:
#                 qs = Suggestion.objects.filter(user_id=u, object_id=movie_id, content_type=ctype)
#                 obj = qs.first()
#                 to_delete = qs.exclude(id=obj.id)
#                 to_delete.delete()
#             if obj.value != pred:
#                 obj.value = pred
#                 obj.save()
#     if end_page < max_pages:
#         return batch_users_prediction_task(start_page=end_page-1)
