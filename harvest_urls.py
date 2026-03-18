"""
harvest_urls.py
---------------
Crawls the High Point Market exhibitor directory and collects all individual
exhibitor URLs into urls.xlsx.

Strategy:
  - The directory paginates via ?pageindex=N (10 results per page)
  - We iterate pages 1, 2, 3... until a page returns no exhibitor links
  - No alpha filtering needed — just page through everything sequentially
  - Parallel workers each handle a chunk of page numbers
"""

import asyncio
import re
import time
import urllib.parse

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
import openpyxl

BASE_URL      = "https://www.highpointmarket.org"
DIRECTORY_URL = f"{BASE_URL}/exhibitordirectory"

CONCURRENCY  = 3          # parallel browser tabs
OUTPUT_FILE  = "urls.xlsx"
LOAD_TIMEOUT = 60_000     # 60s for initial page load
SEL_TIMEOUT  = 15_000     # 15s waiting for exhibitor links


async def get_urls_for_page(browser, page_num: int) -> list[str] | None:
    """
    Scrape one directory page. Returns:
      - list of URLs found (may be empty if page has no results → signals end)
      - None on error (will be retried by caller)
    """
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
    )
    page = await context.new_page()

    try:
        url = f"{DIRECTORY_URL}?pageindex={page_num}"
        await page.goto(url, wait_until="domcontentloaded", timeout=LOAD_TIMEOUT)
        await page.wait_for_timeout(2_500)  # let JS render the cards

        # Check if any exhibitor links loaded
        try:
            await page.wait_for_selector("a[href*='/exhibitor/']", timeout=SEL_TIMEOUT)
        except PWTimeoutError:
            return []  # no results — signals end of pagination

        hrefs = await page.locator("a[href*='/exhibitor/']").evaluate_all(
            "els => els.map(e => e.getAttribute('href'))"
        )
        urls = []
        for href in hrefs:
            if href and re.match(r"^/exhibitor/\d+$", href):
                urls.append(f"{BASE_URL}{href}")

        return urls

    except Exception as e:
        print(f"  [page {page_num}] ERROR: {str(e)[:100]}")
        return None  # signals retry

    finally:
        try:
            await context.close()
        except Exception:
            pass


async def harvest_all() -> list[str]:
    all_urls: set[str] = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )

        # We don't know total pages upfront, so use a shared counter
        # and stop when we hit an empty page
        page_num = 1
        page_lock = asyncio.Lock()
        stop_event = asyncio.Event()
        results_lock = asyncio.Lock()

        async def worker(worker_id: int):
            nonlocal page_num
            while not stop_event.is_set():
                async with page_lock:
                    if stop_event.is_set():
                        break
                    current = page_num
                    page_num += 1

                # Retry up to 3 times on error
                urls = None
                for attempt in range(1, 4):
                    urls = await get_urls_for_page(browser, current)
                    if urls is not None:
                        break
                    if attempt < 3:
                        print(f"  [page {current}] retrying (attempt {attempt+1})...")
                        await asyncio.sleep(3)

                if urls is None:
                    print(f"  [page {current}] failed after 3 attempts, skipping")
                    continue

                if len(urls) == 0:
                    print(f"  [page {current}] empty — stopping")
                    stop_event.set()
                    break

                async with results_lock:
                    all_urls.update(urls)

                print(f"  [page {current}] found {len(urls)} exhibitors"
                      f" (total so far: {len(all_urls)})")

        workers = [asyncio.create_task(worker(i + 1)) for i in range(CONCURRENCY)]
        await asyncio.gather(*workers)

        await browser.close()

    return sorted(all_urls)


def save_to_excel(urls: list[str], path: str):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Exhibitor URLs"

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
    print(f"Paginating through directory with concurrency={CONCURRENCY}...\n")
    t0 = time.time()

    urls = await harvest_all()

    elapsed = time.time() - t0
    print(f"\nTotal unique exhibitor URLs found: {len(urls)}")
    print(f"Elapsed: {elapsed:.1f}s")

    if urls:
        save_to_excel(urls, OUTPUT_FILE)
    else:
        print("WARNING: No URLs found — check if the site structure has changed.")


if __name__ == "__main__":
    asyncio.run(main())
