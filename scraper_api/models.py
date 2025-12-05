# scraper_api/models.py
from uuid import uuid4
import hashlib
from django.db import models, transaction
from django.utils import timezone
from django.core.validators import MinValueValidator


def compute_quote_hash(text: str, author_name: str | None) -> str:
    """
    Normalizes minimaly and returns a sha256 hex digest.
    Keeps function pure and testable.
    """
    norm_text = " ".join((text or "").strip().split()).lower()
    norm_author = (author_name or "").strip().lower()
    payload = f"{norm_text}|{norm_author}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class TimeStampedModel(models.Model):
    """Reusable timestamp mixin."""
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class ScrapeSite(TimeStampedModel):
    """
    Configuration for an allowed/known scrape target.
    Parsers/workers read selectors + options from here.
    """
    name = models.CharField(max_length=150, unique=True)
    base_url = models.URLField(max_length=500)         # e.g. https://quotes.toscrape.com
    start_path = models.CharField(max_length=500, blank=True, default="/")
    # CSS/XPath selectors or simple heuristics — keep parser code flexible to accept either.
    quote_selector = models.CharField(max_length=500, help_text="CSS or XPath for quote text")
    author_selector = models.CharField(max_length=500, help_text="CSS or XPath for author name")
    pagination_selector = models.CharField(max_length=500, blank=True, default="", help_text="CSS/XPath for next page link (optional)")
    max_pages = models.PositiveIntegerField(default=50, validators=[MinValueValidator(1)])
    rate_limit_ms = models.PositiveIntegerField(default=500, help_text="Delay between requests in milliseconds")
    active = models.BooleanField(default=True)

    def __str__(self) -> str:
        return self.name


class Author(TimeStampedModel):
    """
    Unique authors deduped by name.
    Can be expanded later (bio, born_date etc).
    """
    name = models.CharField(max_length=250, unique=True)
    bio_url = models.URLField(blank=True, null=True)

    def __str__(self) -> str:
        return self.name


class ScrapeJob(TimeStampedModel):
    """
    Represents a single scraping execution. Use UUID PK for easier cross-service referencing.
    Status lifecycle: PENDING -> RUNNING -> SUCCESS / PARTIAL_SUCCESS / FAILED
    """
    class Status(models.TextChoices):
        PENDING = "PENDING", "Pending"
        RUNNING = "RUNNING", "Running"
        SUCCESS = "SUCCESS", "Success"
        PARTIAL = "PARTIAL", "Partial Success"
        FAILED = "FAILED", "Failed"

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    site = models.ForeignKey(ScrapeSite, on_delete=models.PROTECT, related_name="jobs")
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    started_at = models.DateTimeField(blank=True, null=True)
    finished_at = models.DateTimeField(blank=True, null=True)
    quotes_fetched = models.IntegerField(default=0)
    quotes_saved = models.IntegerField(default=0)
    errors_count = models.IntegerField(default=0)
    # who triggered it (api / scheduler / manual)
    initiator = models.CharField(max_length=40, default="api")
    # flexible metadata: pages scraped list, headers used, durations etc.
    meta = models.JSONField(blank=True, null=True)

    def mark_running(self):
        self.status = self.Status.RUNNING
        self.started_at = timezone.now()
        self.save(update_fields=["status", "started_at", "updated_at"])

    def mark_finished(self, success: bool = True):
        """
        Set finished time and status depending on counters.
        If there were errors but some quotes saved -> PARTIAL
        If success is False and no quotes saved -> FAILED
        """
        self.finished_at = timezone.now()
        if not success:
            # Caller can set success=False for fatal failures
            self.status = self.Status.FAILED
        else:
            if self.errors_count > 0 and self.quotes_saved > 0:
                self.status = self.Status.PARTIAL
            elif self.quotes_saved > 0:
                self.status = self.Status.SUCCESS
            else:
                self.status = self.Status.FAILED
        self.save(update_fields=["status", "finished_at", "updated_at"])

    def increment_counters(self, fetched: int = 0, saved: int = 0, errors: int = 0):
        """
        Safe counter increments via a transaction to avoid races.
        Call from worker tasks after each page or batch.
        """
        with transaction.atomic():
            # Refetch for DB-level consistency
            job = ScrapeJob.objects.select_for_update().get(pk=self.pk)
            job.quotes_fetched = job.quotes_fetched + fetched
            job.quotes_saved = job.quotes_saved + saved
            job.errors_count = job.errors_count + errors
            job.save(update_fields=["quotes_fetched", "quotes_saved", "errors_count", "updated_at"])

    def __str__(self) -> str:
        return f"Job {self.pk} - {self.site.name} - {self.status}"


class ScrapeError(TimeStampedModel):
    """
    Detailed per-url/parsing errors. Keeps ScrapeJob lean.
    """
    job = models.ForeignKey(ScrapeJob, on_delete=models.CASCADE, related_name="errors")
    url = models.URLField(max_length=1000, blank=True, null=True)
    error_type = models.CharField(max_length=120)   # network / parse / timeout etc.
    message = models.TextField(blank=True)
    traceback = models.TextField(blank=True, null=True)

    def __str__(self) -> str:
        return f"{self.error_type} @ {self.url or 'unknown'}"


class Quote(TimeStampedModel):
    """
    Normalized quote entity.
    Hash ensures idempotency across retries & parallel workers.
    The site FK preserves origin (for analytics / provenance).
    """
    text = models.TextField()
    author = models.ForeignKey(Author, on_delete=models.SET_NULL, null=True, blank=True, related_name="quotes")
    site = models.ForeignKey(ScrapeSite, on_delete=models.PROTECT, related_name="quotes")
    source_url = models.URLField(max_length=1000, blank=True, null=True)
    hash = models.CharField(max_length=64, unique=True, db_index=True)  # sha256 hex
    saved_by_job = models.ForeignKey(ScrapeJob, on_delete=models.SET_NULL, null=True, blank=True, related_name="saved_quotes")

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["author"]),
            models.Index(fields=["site"]),
        ]

    def save(self, *args, **kwargs):
        # Ensure hash is set before saving if missing.
        if not self.hash:
            author_name = self.author.name if self.author else ""
            self.hash = compute_quote_hash(self.text, author_name)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        short = (self.text[:60] + "...") if len(self.text) > 60 else self.text
        return f"Quote({short}) - {self.author.name if self.author else 'Unknown'}"
