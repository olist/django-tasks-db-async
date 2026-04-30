from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "django-insecure-key"  # noqa: S105

DEBUG = True

ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django_tasks_db",
    "django_tasks_db_async",
    "tests",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
        "CONN_MAX_AGE": 0,
        "TEST": {
            "NAME": BASE_DIR / "test_db.sqlite3",
        },
    },
}

USE_TZ = True

TASKS = {
    "default": {
        "BACKEND": "django_tasks_db.DatabaseBackend",
        "QUEUES": ["default"],
    },
}
