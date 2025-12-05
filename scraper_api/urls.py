# scraper_api/urls.py
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
# Register core viewsets (you'll implement these in scraper_api/views.py)
router.register(r"sites", views.ScrapeSiteViewSet, basename="scrapesite")
router.register(r"authors", views.AuthorViewSet, basename="author")
router.register(r"quotes", views.QuoteViewSet, basename="quote")
router.register(r"jobs", views.ScrapeJobViewSet, basename="scrapejob")
router.register(r"errors", views.ScrapeErrorViewSet, basename="scrapeerror")

# Extra custom endpoints can be added as actions on viewsets.
# Example: POST /api/scrape/<job_id>/start/ could be a @action on ScrapeJobViewSet.
urlpatterns = [
    path("", include(router.urls)),
]
