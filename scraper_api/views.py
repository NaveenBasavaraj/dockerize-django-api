# scraper_api/views.py
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from django.shortcuts import get_object_or_404

from .models import ScrapeSite, Author, Quote, ScrapeJob, ScrapeError
from .serializers import (
    ScrapeSiteSerializer,
    AuthorSerializer,
    QuoteSerializer,
    ScrapeJobSerializer,
    ScrapeErrorSerializer,
)

# Celery task to enqueue a scrape job
from .tasks import enqueue_scrape_job


class ScrapeSiteViewSet(viewsets.ModelViewSet):
    queryset = ScrapeSite.objects.all()
    serializer_class = ScrapeSiteSerializer
    filterset_fields = ["active", "name"]


class AuthorViewSet(viewsets.ModelViewSet):
    queryset = Author.objects.all()
    serializer_class = AuthorSerializer
    search_fields = ["name"]


class QuoteViewSet(viewsets.ModelViewSet):
    queryset = Quote.objects.select_related("author", "site").all()
    serializer_class = QuoteSerializer
    filterset_fields = ["site", "author"]
    search_fields = ["text"]


class ScrapeErrorViewSet(viewsets.ModelViewSet):
    queryset = ScrapeError.objects.select_related("job").all()
    serializer_class = ScrapeErrorSerializer
    filterset_fields = ["job", "error_type"]


class ScrapeJobViewSet(viewsets.ModelViewSet):
    queryset = ScrapeJob.objects.select_related("site").all().order_by("-created_at")
    serializer_class = ScrapeJobSerializer
    filterset_fields = ["site", "status", "initiator"]

    def create(self, request, *args, **kwargs):
        """
        Create job (via serializer) and immediately enqueue the Celery job.
        Serializer.create already creates DB row; here we enqueue the worker.
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        job = serializer.save()
        # enqueue async job
        enqueue_scrape_job.delay(str(job.id))
        headers = self.get_success_headers(serializer.data)
        return Response(serializer.data, status=status.HTTP_201_CREATED, headers=headers)

    @action(detail=True, methods=["post"])
    def start(self, request, pk=None):
        """
        Explicit start endpoint: useful if you want to create without starting,
        then start later. This will enqueue the job if status is PENDING.
        """
        job = get_object_or_404(ScrapeJob, pk=pk)
        if job.status != ScrapeJob.Status.PENDING:
            return Response({"detail": "Job not in PENDING state."}, status=status.HTTP_400_BAD_REQUEST)
        enqueue_scrape_job.delay(str(job.id))
        return Response({"detail": "Job enqueued."}, status=status.HTTP_202_ACCEPTED)
