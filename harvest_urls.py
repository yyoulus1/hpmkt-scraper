"""
harvest_urls.py
---------------
Crawls the High Point Market exhibitor directory and collects all individual
exhibitor URLs into urls.xlsx.

The directory at https://www.highpointmarket.org/ExhibitorDirectory is a
JavaScript-rendered React/SPA page. We use Playwright to render it, then
iterate A–Z (plus 0–9) alpha filters to paginate through all exhibitors.

Run locally:
    pip install playwright openpyxl
    playwright install chromium
    python harvest_urls.py

Output: urls.xlsx (one URL per row, column A)
"""

import asyncio
import json
import re
import time
from pathlib import Path

from playwright.async_api import async_playwright
import openpyxl

BASE_URL = "https://www.highpointmarket.org"
DIRECTORY_URL = f"{BASE_URL}/ExhibitorDirectory"

# Alpha buckets to iterate — covers A-Z plus digits/symbols bucket
ALPHA_LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0-9"]

CONCURRENCY = 3          # parallel browser tabs for harvesting
OUTPUT_FILE = "urls.xlsx"
TIMEOUT_MS  = 30_000     # 30s page load timeout


async def get_urls_for_letter(context, letter: str) -> list[str]:
    """Open one directory page filtered by alpha letter and collect all exhibitor links."""
    page = await context.new_page()
    try:
        # Build the filter URL — matches the JSON blob the site uses
        filter_payload = json.dumps({"Type": "Alpha", "Values": [letter]})
        url = f"{DIRECTORY_URL}?filters={filter_payload}"
        await page.goto(url, wait_until="networkidle", timeout=TIMEOUT_MS)

        # Wait for exhibitor cards to appear — they live in anchor tags with /exhibitor/ hrefs
        try:
            await page.wait_for_selector("a[href*='/exhibitor/']", timeout=TIMEOUT_MS)
        except Exception:
            # No results for this letter — that's fine
            return []

        # Scroll to bottom to trigger any lazy-load / infinite scroll
        prev_count = 0
        for _ in range(20):  # max 20 scroll attempts
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1500)

            # Check for a "Load More" button and click it if present
            load_more = page.locator("button:has-text('Load More'), a:has-text('Load More')")
            if await load_more.count() > 0:
                try:
                    await load_more.first.click(timeout=5_000)
                    await page.wait_for_timeout(1500)
                except Exception:
                    pass

            links = await page.locator("a[href*='/exhibitor/']").all()
            if len(links) == prev_count:
                break  # no new content loaded
            prev_count = len(links)

        # Extract all unique /exhibitor/<id> hrefs
        hrefs = await page.locator("a[href*='/exhibitor/']").evaluate_all(
            "els => els.map(e => e.getAttribute('href'))"
        )
        urls = set()
        for href in hrefs:
            if href and re.match(r"^/exhibitor/\d+$", href):
                urls.add(f"{BASE_URL}{href}")

        print(f"  [{letter}] found {len(urls)} exhibitors")
        return sorted(urls)

    except Exception as e:
        print(f"  [{letter}] ERROR: {e}")
        return []
    finally:
        await page.close()


async def harvest_all() -> list[str]:
    """Iterate all alpha buckets and return a deduplicated, sorted list of URLs."""
    all_urls: set[str] = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )

        # Process letters in batches of CONCURRENCY
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def bounded(letter):
            async with semaphore:
                return await get_urls_for_letter(context, letter)

        tasks = [bounded(letter) for letter in ALPHA_LETTERS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, list):
                all_urls.update(result)

        await browser.close()

    return sorted(all_urls)


def save_to_excel(urls: list[str], path: str):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Exhibitor URLs"

    # Header
    ws["A1"] = "URL"
    ws["A1"].font = openpyxl.styles.Font(bold=True, color="FFFFFF")
    ws["A1"].fill = openpyxl.styles.PatternFill("solid", fgColor="1F3864")

    for i, url in enumerate(urls, start=2):
        ws.cell(row=i, column=1, value=url)

    ws.column_dimensions["A"].width = 60
    wb.save(path)
    print(f"\nSaved {len(urls)} URLs → {path}")


async def main():
    print("=== High Point Market — URL Harvester ===")
    print(f"Crawling {len(ALPHA_LETTERS)} alpha buckets with concurrency={CONCURRENCY}...\n")
    t0 = time.time()

    urls = await harvest_all()

    elapsed = time.time() - t0
    print(f"\nTotal unique exhibitor URLs found: {len(urls)}")
    print(f"Elapsed: {elapsed:.1f}s")

    save_to_excel(urls, OUTPUT_FILE)


if __name__ == "__main__":
    asyncio.run(main())
