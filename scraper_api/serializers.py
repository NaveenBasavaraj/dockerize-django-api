# scraper_api/serializers.py
from typing import Any, Dict, Optional
from django.utils import timezone
from rest_framework import serializers
from .models import ScrapeSite, Author, Quote, ScrapeJob, ScrapeError


class ScrapeSiteSerializer(serializers.ModelSerializer):
    class Meta:
        model = ScrapeSite
        fields = [
            "id",
            "name",
            "base_url",
            "start_path",
            "quote_selector",
            "author_selector",
            "pagination_selector",
            "max_pages",
            "rate_limit_ms",
            "active",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]


class AuthorSerializer(serializers.ModelSerializer):
    class Meta:
        model = Author
        fields = ["id", "name", "bio_url", "created_at", "updated_at"]
        read_only_fields = ["id", "created_at", "updated_at"]


class QuoteSerializer(serializers.ModelSerializer):
    # accept either author id (existing) or nested author object {name: "..."}
    author = serializers.PrimaryKeyRelatedField(
        queryset=Author.objects.all(), required=False, allow_null=True
    )
    author_name = serializers.CharField(
        write_only=True, required=False, allow_blank=True, help_text="Use when creating an author inline."
    )
    site = serializers.PrimaryKeyRelatedField(queryset=ScrapeSite.objects.filter(active=True))
    saved_by_job = serializers.PrimaryKeyRelatedField(queryset=ScrapeJob.objects.all(), required=False, allow_null=True)

    class Meta:
        model = Quote
        fields = [
            "id",
            "text",
            "author",
            "author_name",
            "site",
            "source_url",
            "hash",
            "saved_by_job",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "hash", "created_at", "updated_at"]

    def validate(self, attrs: Dict[str, Any]) -> Dict[str, Any]:
        # ensure we have either author or author_name (or allow both null)
        author = attrs.get("author")
        author_name = attrs.get("author_name", "").strip()
        if not author and not author_name:
            # allow unknown authors if user explicitly wants to leave blank
            # you can tighten this rule if you want authors to be required
            return attrs
        return attrs

    def create(self, validated_data: Dict[str, Any]) -> Quote:
        author = validated_data.pop("author", None)
        author_name = validated_data.pop("author_name", "").strip()

        # create or get author if name provided
        if not author and author_name:
            author, _ = Author.objects.get_or_create(name=author_name)

        # do not allow creating quote with inactive site
        site = validated_data.get("site")
        if not site.active:
            raise serializers.ValidationError({"site": "Specified site is not active for scraping."})

        # Compute hash will be handled in model.save() if missing
        quote = Quote(author=author, **validated_data)
        quote.save()
        return quote


class ScrapeErrorSerializer(serializers.ModelSerializer):
    class Meta:
        model = ScrapeError
        fields = ["id", "job", "url", "error_type", "message", "traceback", "created_at", "updated_at"]
        read_only_fields = ["id", "created_at", "updated_at"]


class ScrapeJobSerializer(serializers.ModelSerializer):
    site = serializers.PrimaryKeyRelatedField(queryset=ScrapeSite.objects.filter(active=True))
    # nicer status output for read operations
    status_display = serializers.SerializerMethodField(read_only=True)
    meta = serializers.JSONField(required=False)

    class Meta:
        model = ScrapeJob
        fields = [
            "id",
            "site",
            "status",
            "status_display",
            "started_at",
            "finished_at",
            "quotes_fetched",
            "quotes_saved",
            "errors_count",
            "initiator",
            "meta",
            "created_at",
            "updated_at",
        ]
        read_only_fields = [
            "id",
            "status",
            "started_at",
            "finished_at",
            "quotes_fetched",
            "quotes_saved",
            "errors_count",
            "created_at",
            "updated_at",
        ]

    def get_status_display(self, obj: ScrapeJob) -> str:
        return obj.get_status_display() if hasattr(obj, "get_status_display") else str(obj.status)

    def validate(self, attrs: Dict[str, Any]) -> Dict[str, Any]:
        # optional: ensure meta is a dict if provided
        meta = attrs.get("meta")
        if meta is not None and not isinstance(meta, dict):
            raise serializers.ValidationError({"meta": "meta must be a JSON object/dict."})
        return attrs

    def create(self, validated_data: Dict[str, Any]) -> ScrapeJob:
        """
        Creates a ScrapeJob (PENDING) and enqueues the Celery task to run it.
        The actual celery call is commented — wire it to your task name.
        """
        site = validated_data.pop("site")
        initiator = validated_data.get("initiator", "api")
        meta = validated_data.get("meta", {})

        job = ScrapeJob.objects.create(site=site, initiator=initiator, meta=meta)
        # job is created with status PENDING by default (model default)

        # Optionally, enqueue Celery task to perform the job. Uncomment and adapt to your task name.
        # from .tasks import enqueue_scrape_job  # your celery entrypoint
        # enqueue_scrape_job.delay(str(job.id))  # or pass site.id / meta / options as needed

        return job

    def update(self, instance: ScrapeJob, validated_data: Dict[str, Any]) -> ScrapeJob:
        # allow updating meta and initiator via PATCH, but not status directly (workers should update status)
        meta = validated_data.get("meta")
        if meta is not None:
            instance.meta = meta
        initiator = validated_data.get("initiator")
        if initiator is not None:
            instance.initiator = initiator
        instance.updated_at = timezone.now()
        instance.save(update_fields=["meta", "initiator", "updated_at"])
        return instance
