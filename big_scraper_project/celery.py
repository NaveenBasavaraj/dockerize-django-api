# big_scraper_project/celery.py
import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "big_scraper_project.settings")

app = Celery("big_scraper_project")
# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object("django.conf:settings", namespace="CELERY")
# Autodiscover tasks in INSTALLED_APPS
app.autodiscover_tasks()
