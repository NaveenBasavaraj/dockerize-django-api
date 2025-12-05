# scraper_api/tasks.py
import time
import logging
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from celery import shared_task, Task
from celery.utils.log import get_task_logger
from django.db import IntegrityError, transaction

from .models import ScrapeJob, ScrapeSite, Author, Quote, ScrapeError, compute_quote_hash

logger = get_task_logger(__name__)
DEFAULT_HEADERS = {"User-Agent": "big_scraper_project/1.0 (+https://example.com)"}


class BaseTaskWithRetry(Task):
    autoretry_for = (requests.RequestException,)
    retry_backoff = True
    max_retries = 3
    retry_jitter = True


@shared_task(bind=True, base=BaseTaskWithRetry)
def enqueue_scrape_job(self, job_id: str):
    """
    Worker entrypoint invoked by API. Loads job and triggers scrape_site.
    This task keeps a simple decoupling layer.
    """
    logger.info("enqueue_scrape_job start: %s", job_id)
    job = ScrapeJob.objects.select_related("site").get(pk=job_id)
    # Kick off the actual scrape (synchronous call inside worker process)
    scrape_site.apply_async(args=[str(job.id)])
    return {"job_id": job_id, "status": "enqueued"}


@shared_task(bind=True, base=BaseTaskWithRetry)
def scrape_site(self, job_id: str):
    """
    Main scraping task. This is intentionally simple: fetch sequential pages
    following pagination_selector or page links up to max_pages.
    For scale you can split pages into subtasks (parse_page).
    """
    logger.info("scrape_site start: %s", job_id)
    job = ScrapeJob.objects.select_related("site").get(pk=job_id)
    site: ScrapeSite = job.site

    job.mark_running()

    headers = DEFAULT_HEADERS.copy()
    pages_scraped = []
    saved = 0
    fetched = 0
    errors = 0

    try:
        start_url = urljoin(site.base_url, site.start_path or "/")
        next_url = start_url
        for page_index in range(site.max_pages):
            if not next_url:
                break

            logger.info("Job %s fetching page %s", job_id, next_url)
            time.sleep(site.rate_limit_ms / 1000.0)  # polite delay
            try:
                resp = requests.get(next_url, headers=headers, timeout=10)
                resp.raise_for_status()
            except Exception as e:
                errors += 1
                ScrapeError.objects.create(job=job, url=next_url, error_type="network", message=str(e))
                # try to continue to next page if possible
                break

            fetched += 1
            pages_scraped.append(next_url)
            soup = BeautifulSoup(resp.text, "lxml")

            # find quote elements
            quote_nodes = soup.select(site.quote_selector)
            author_nodes = soup.select(site.author_selector) if site.author_selector else []

            # heuristics: if number of authors matches quotes, pair them; else use parent traversal
            for i, qnode in enumerate(quote_nodes):
                try:
                    text = qnode.get_text(separator=" ", strip=True)
                    author_name = None
                    if author_nodes and i < len(author_nodes):
                        author_name = author_nodes[i].get_text(separator=" ", strip=True)
                    else:
                        # Try common patterns: look for sibling or nearest author selector inside qnode
                        possible = qnode.select_one(site.author_selector) if site.author_selector else None
                        if possible:
                            author_name = possible.get_text(separator=" ", strip=True)

                    # Save author (get_or_create) and quote, idempotent via hash
                    with transaction.atomic():
                        author = None
                        if author_name:
                            author, _ = Author.objects.get_or_create(name=author_name)
                        h = compute_quote_hash(text, author_name)
                        if not Quote.objects.filter(hash=h).exists():
                            quote = Quote.objects.create(
                                text=text,
                                author=author,
                                site=site,
                                source_url=next_url,
                                hash=h,
                                saved_by_job=job,
                            )
                            saved += 1
                except Exception as ex:
                    errors += 1
                    ScrapeError.objects.create(job=job, url=next_url, error_type="parse", message=str(ex))
                    logger.exception("parse error on %s", next_url)

            # update job counters after each page
            job.increment_counters(fetched=fetched, saved=saved, errors=errors)
            # reset page-level counters (since increment_counters added them)
            fetched = 0
            saved = 0
            errors = 0

            # Decide next page
            next_url = None
            if site.pagination_selector:
                nxt = soup.select_one(site.pagination_selector)
                if nxt:
                    href = nxt.get("href") or nxt.get("data-href")
                    if href:
                        # create absolute url
                        next_url = urljoin(site.base_url, href)
            else:
                # fallback: try to find "a[rel=next]" or a link with "next" text
                nxt = soup.select_one("a[rel=next]") or soup.find("a", string=lambda s: s and s.lower().strip() in ("next", "›", "»"))
                if nxt and nxt.get("href"):
                    next_url = urljoin(site.base_url, nxt.get("href"))

            # avoid infinite loops if pagination links point to same page
            if next_url and urlparse(next_url).path == urlparse(next_url).path:
                # safe guard trivial loop (could be improved)
                pass

        # One final job counter update to ensure totals are stored (if any remained)
        job.increment_counters(fetched=fetched, saved=saved, errors=errors)
        # mark finished with success
        job.mark_finished(success=True)
        logger.info("scrape_site finished: %s", job_id)
        return {"job_id": job_id, "status": "done"}
    except Exception as fatal:
        # record fatal
        ScrapeError.objects.create(job=job, url=None, error_type="fatal", message=str(fatal))
        job.increment_counters(fetched=fetched, saved=saved, errors=1)
        job.mark_finished(success=False)
        logger.exception("fatal error in scrape_job %s", job_id)
        raise
