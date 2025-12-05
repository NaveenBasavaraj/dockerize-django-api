# big_scraper_project/urls.py
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path("admin/", admin.site.urls),
    # API root for your scraper app
    path("api/", include("scraper_api.urls")),
    # Optionally expose DRF auth for browsable API (remove in prod if you don't want it)
    path("api-auth/", include("rest_framework.urls", namespace="rest_framework")),
]
