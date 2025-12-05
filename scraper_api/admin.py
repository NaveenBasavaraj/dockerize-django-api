# scraper_api/admin.py
from django.contrib import admin
from .models import ScrapeSite, Author, Quote, ScrapeJob, ScrapeError


@admin.register(ScrapeSite)
class ScrapeSiteAdmin(admin.ModelAdmin):
    list_display = ("name", "base_url", "active", "max_pages", "rate_limit_ms", "created_at")
    search_fields = ("name", "base_url")
    list_filter = ("active",)


@admin.register(Author)
class AuthorAdmin(admin.ModelAdmin):
    list_display = ("name", "bio_url", "created_at")
    search_fields = ("name",)


@admin.register(Quote)
class QuoteAdmin(admin.ModelAdmin):
    list_display = ("short_text", "author", "site", "created_at")
    search_fields = ("text", "author__name")
    list_filter = ("site",)

    def short_text(self, obj):
        return (obj.text[:75] + "...") if len(obj.text) > 75 else obj.text
    short_text.short_description = "Quote"


@admin.register(ScrapeJob)
class ScrapeJobAdmin(admin.ModelAdmin):
    list_display = ("id", "site", "status", "initiator", "started_at", "finished_at", "quotes_fetched", "quotes_saved", "errors_count")
    list_filter = ("status", "site", "initiator")


@admin.register(ScrapeError)
class ScrapeErrorAdmin(admin.ModelAdmin):
    list_display = ("job", "error_type", "url", "created_at")
    list_filter = ("error_type",)
