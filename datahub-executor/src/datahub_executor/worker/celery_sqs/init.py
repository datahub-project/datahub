from celery import Celery

# This has to be isolated in a separate file for tests to work properly
app = Celery("tasks")
