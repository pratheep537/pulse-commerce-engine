import asyncio
from dataclasses import asdict, dataclass
from decimal import Decimal
import json
import logging
import os
from pathlib import Path
import re
import uuid

from playwright.async_api import Browser, BrowserContext, Page, Response, async_playwright
import psycopg2
from psycopg2.extras import Json, execute_values

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("pulse.pipeline")

TARGET_URL = os.getenv(
    "TARGET_URL",
    "https://webscraper.io/test-sites/e-commerce/ajax/computers/laptops",
)
BASE_DOMAIN = "https://webscraper.io"
ZONES = ("BLR-KORAMANGALA", "CHN-ADYAR", "CBE-PNPALAYAM")
MAX_CONCURRENT_CONTEXTS = int(os.getenv("MAX_CONCURRENT_CONTEXTS", "2"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "3"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", "artifacts"))
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pulse:pulse@localhost:5432/warehouse",
)


@dataclass(slots=True)
class ScrapedSKU:
    zone_id: str
    product_url: str
    title: str
    price: Decimal
    description: str
    reviews_count: int
    rating: int
    in_stock: bool = True


def _parse_price(raw_price: str) -> Decimal:
    cleaned = re.sub(r"[^\d.]", "", raw_price)
    return Decimal(cleaned) if cleaned else Decimal("0.00")


def _parse_reviews(raw_reviews: str) -> int:
    match = re.search(r"\d+", raw_reviews)
    return int(match.group(0)) if match else 0


async def _extract_page_items(page: Page, zone_id: str) -> list[ScrapedSKU]:
    cards = page.locator(".ecomerce-items-ajax .card.thumbnail, .card.thumbnail")
    count = await cards.count()
    items: list[ScrapedSKU] = []

    for idx in range(count):
        card = cards.nth(idx)
        title_link = card.locator("a.title").first
        href = (await title_link.get_attribute("href")) or ""
        product_url = f"{BASE_DOMAIN}{href}" if href.startswith("/") else href
        title = (
            await title_link.get_attribute("title")
            or await title_link.inner_text()
        ).strip()

        price_text = (await card.locator(".price").first.inner_text()).strip()
        desc_text = (await card.locator(".description").first.inner_text()).strip()

        reviews_loc = card.locator(".ratings .review-count, .ratings p.pull-right")
        reviews_text = (
            (await reviews_loc.first.inner_text()).strip()
            if await reviews_loc.count() > 0
            else "0"
        )

        rating_loc = card.locator(".ratings [data-rating]")
        if await rating_loc.count() > 0:
            rating = int(await rating_loc.first.get_attribute("data-rating") or 0)
        else:
            rating = await card.locator(".ratings .ws-icon-star, .ratings .glyphicon-star").count()

        items.append(
            ScrapedSKU(
                zone_id=zone_id,
                product_url=product_url,
                title=title,
                price=_parse_price(price_text),
                description=desc_text,
                reviews_count=_parse_reviews(reviews_text),
                rating=rating,
            )
        )
    return items


async def _scrape_zone_once(
    browser: Browser,
    zone_id: str,
    run_id: str,
) -> tuple[list[tuple[int, int | None, list[dict]]], list[ScrapedSKU]]:
    context: BrowserContext = await browser.new_context(
        viewport={"width": 1280, "height": 800},
        timezone_id="Asia/Kolkata",
        extra_http_headers={"X-Pulse-Zone": zone_id},
    )
    await context.tracing.start(screenshots=True, snapshots=True, sources=True)
    trace_saved = False

    try:
        page = await context.new_page()
        xhr_statuses: list[int] = []

        def on_response(response: Response) -> None:
            if response.request.resource_type in ("xhr", "fetch"):
                xhr_statuses.append(response.status)

        page.on("response", on_response)

        initial_resp = await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector(".card.thumbnail a.title", timeout=15000)

        bronze_pages: list[tuple[int, int | None, list[dict]]] = []
        all_items: list[ScrapedSKU] = []

        for page_num in range(1, MAX_PAGES + 1):
            if page_num > 1:
                next_btn = page.locator(
                    f"ul.pagination li.page-item a.page-link:text-is('{page_num}'), "
                    f"ul.pagination li a:text-is('{page_num}'), "
                    f"ul.pagination button:text-is('{page_num}')"
                )
                if await next_btn.count() == 0:
                    logger.info("[%s] No page %d button found; stopping pagination.", zone_id, page_num)
                    break

                prev_href = await page.locator(".card.thumbnail a.title").first.get_attribute("href")
                xhr_statuses.clear()
                await next_btn.first.click()

                # Deterministic AJAX completion check: active tab & first SKU href both change
                await page.wait_for_function(
                    """([prevHref, targetPage]) => {
                        const active = document.querySelector('ul.pagination li.active');
                        const firstLink = document.querySelector('.card.thumbnail a.title');
                        return Boolean(
                            active &&
                            active.innerText.trim() === String(targetPage) &&
                            firstLink &&
                            firstLink.getAttribute('href') !== prevHref
                        );
                    }""",
                    arg=[prev_href, page_num],
                    timeout=15000,
                )

            items = await _extract_page_items(page, zone_id)
            status_code = (
                xhr_statuses[-1]
                if xhr_statuses
                else (initial_resp.status if initial_resp else None)
            )

            raw_payload = [
                {**asdict(item), "price": str(item.price)}
                for item in items
            ]
            bronze_pages.append((page_num, status_code, raw_payload))
            all_items.extend(items)
            logger.info("[%s] Page %d scraped (%d SKUs, status=%s)", zone_id, page_num, len(items), status_code)

        # Success: discard in-memory trace without writing to disk
        await context.tracing.stop()
        return bronze_pages, all_items

    except Exception as exc:
        ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
        trace_path = ARTIFACTS_DIR / f"trace_{zone_id}_{run_id}.zip"
        await context.tracing.stop(path=str(trace_path))
        trace_saved = True
        logger.error("[%s] Scrape failed (%s). Forensic trace saved: %s", zone_id, exc, trace_path)
        raise
    finally:
        if not trace_saved:
            try:
                await context.tracing.stop()
            except Exception:
                pass
        await context.close()


def _persist_zone_sync(
    run_id: str,
    zone_id: str,
    bronze_pages: list[tuple[int, int | None, list[dict]]],
    items: list[ScrapedSKU],
) -> None:
    # Deduplicate by composite PK (zone_id, product_url) before ON CONFLICT DO UPDATE
    deduped: dict[tuple[str, str], ScrapedSKU] = {
        (item.zone_id, item.product_url): item for item in items
    }

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO bronze_raw_scrapes (run_id, zone_id, page_num, xhr_status, payload)
                VALUES %s
                """,
                [
                    (run_id, zone_id, page_num, status_code, Json(payload))
                    for page_num, status_code, payload in bronze_pages
                ],
            )

            execute_values(
                cur,
                """
                INSERT INTO silver_latest_skus (
                    zone_id, product_url, title, price, description,
                    reviews_count, rating, in_stock, last_run_id, updated_at
                )
                VALUES %s
                ON CONFLICT (zone_id, product_url) DO UPDATE SET
                    title         = EXCLUDED.title,
                    price         = EXCLUDED.price,
                    description   = EXCLUDED.description,
                    reviews_count = EXCLUDED.reviews_count,
                    rating        = EXCLUDED.rating,
                    in_stock      = EXCLUDED.in_stock,
                    last_run_id   = EXCLUDED.last_run_id,
                    updated_at    = NOW()
                """,
                [
                    (
                        s.zone_id, s.product_url, s.title, s.price, s.description,
                        s.reviews_count, s.rating, s.in_stock, run_id,
                    )
                    for s in deduped.values()
                ],
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
            )

            execute_values(
                cur,
                """
                INSERT INTO silver_price_history (
                    run_id, zone_id, product_url, price, in_stock
                )
                VALUES %s
                """,
                [
                    (run_id, s.zone_id, s.product_url, s.price, s.in_stock)
                    for s in deduped.values()
                ],
            )


async def run_zone_worker(
    semaphore: asyncio.Semaphore,
    browser: Browser,
    zone_id: str,
    run_id: str,
) -> int:
    async with semaphore:
        last_exc: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                bronze_pages, items = await _scrape_zone_once(browser, zone_id, run_id)
                await asyncio.to_thread(_persist_zone_sync, run_id, zone_id, bronze_pages, items)
                logger.info("[%s] Persisted %d SKUs to Bronze/Silver", zone_id, len(items))
                return len(items)
            except Exception as exc:
                last_exc = exc
                if attempt < MAX_RETRIES:
                    backoff = 2 ** attempt
                    logger.warning(
                        "[%s] Attempt %d/%d failed; retrying in %ds...",
                        zone_id, attempt, MAX_RETRIES, backoff,
                    )
                    await asyncio.sleep(backoff)
        raise RuntimeError(f"Zone {zone_id} exhausted {MAX_RETRIES} retries") from last_exc


async def main() -> None:
    run_id = str(uuid.uuid4())
    logger.info("Starting pulse-commerce-engine run_id=%s across %d zones", run_id, len(ZONES))

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONTEXTS)
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            counts = await asyncio.gather(
                *(run_zone_worker(semaphore, browser, zone, run_id) for zone in ZONES)
            )
        finally:
            await browser.close()

    logger.info(
        "Run %s complete. Total SKUs ingested: %d",
        run_id,
        sum(counts),
    )


if __name__ == "__main__":
    asyncio.run(main())