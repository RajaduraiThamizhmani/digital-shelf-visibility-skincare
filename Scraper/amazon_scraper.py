import pandas as pd
import os
import csv
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright

KEYWORDS_FILE = "data/keywords.csv"
OUTPUT_FILE = "output/amazon_output.csv"
MAX_THREADS = 1

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/116 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) Chrome/113 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/102.0",
]

def block_resources(route, request):
    if request.resource_type in ["image", "media", "font"]:
        route.abort()
    else:
        route.continue_()

def human_scroll(page):
    for _ in range(2):
        page.mouse.wheel(0, random.randint(800, 1200))
        page.wait_for_timeout(random.randint(500, 1000))

def scrape_amazon_search_results(keyword, page):
    search_url = f"https://www.amazon.in/s?k={keyword.replace(' ', '+')}"
    print(f"\n🔍 Searching Amazon for: {keyword}")

    try:
        page.goto(search_url, timeout=30000, wait_until="domcontentloaded")
        human_scroll(page)
        page.wait_for_selector('[data-component-type="s-search-result"]', timeout=10000)
    except Exception as e:
        print(f"⚠️ First attempt failed, retrying for: {keyword} — {e}")
        try:
            page.reload()
            human_scroll(page)
            page.wait_for_selector('[data-component-type="s-search-result"]', timeout=10000)
        except Exception as e2:
            print(f"❌ Still failed after retry for: {keyword} — {e2}")
            return []

    # check if robot page
    if "Robot" in page.title() or "captcha" in page.content().lower():
        print(f"🛑 Bot detection page detected for: {keyword}")
        return []

    products = page.locator('[data-component-type="s-search-result"]')
    count = products.count()
    print(f"🧾 Found {count} results.")

    results = []

    for i in range(min(10, count)):
        try:
            item = products.nth(i)

            title = "N/A"
            if item.locator("h2 span").count() > 0:
                title = item.locator("h2 span").inner_text(timeout=1000)

            price_text = item.locator(".a-price .a-offscreen").first.inner_text(timeout=1000) if item.locator(".a-price .a-offscreen").count() else None
            price = float(price_text.replace("₹", "").replace(",", "").strip()) if price_text else None

            mrp_text = item.locator(".a-price.a-text-price[data-a-strike='true'] .a-offscreen").first.inner_text(timeout=1000) if item.locator(".a-price.a-text-price[data-a-strike='true'] .a-offscreen").count() else None
            mrp = float(mrp_text.replace("₹", "").replace(",", "").strip()) if mrp_text else price

            discount_percent = round(((mrp - price) / mrp) * 100, 2) if mrp and price and mrp > price else None

            href = None
            if item.locator("h2 a").count() > 0:
                href = item.locator("h2 a").get_attribute("href", timeout=1000)
            full_link = f"https://www.amazon.in{href}" if href else "N/A"

            stock_status = "In Stock" if price else "Out of Stock"

            sponsored = item.locator("span:has-text('Sponsored')")
            listing_type = "Sponsored" if sponsored.count() > 0 else "Organic"

            print(f"📦 [{i+1}] {title[:50]}... | ₹{price} → ₹{mrp} | {discount_percent}% | {listing_type} | {stock_status}")

            results.append({
                "keyword": keyword,
                "rank": i + 1,
                "listing_type": listing_type,
                "product_name": title,
                "price": price if price else "N/A",
                "mrp": mrp if mrp else "N/A",
                "discount_percent": f"{discount_percent}%" if discount_percent else "N/A",
                "stock_status": stock_status,
                "url": full_link
            })

        except Exception as e:
            print(f"⚠️ Skipping product #{i+1} due to error: {e}")
            continue

    return results

def scrape_keyword_wrapper(keyword):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport=random.choice([
                {"width":1280, "height":800},
                {"width":1440, "height":900},
                {"width":1920, "height":1080}
            ])
        )
        context.route("**/*", block_resources)
        page = context.new_page()

        try:
            return scrape_amazon_search_results(keyword, page)
        except Exception as e:
            print(f"❌ Error scraping '{keyword}': {e}")
            return []
        finally:
            page.close()
            browser.close()

def main():
    os.makedirs("output", exist_ok=True)
    try:
        df = pd.read_csv(KEYWORDS_FILE)
        keywords = df.iloc[:,0].dropna().tolist()
    except Exception as e:
        print(f"❌ Error reading keywords file: {e}")
        return

    all_results = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(scrape_keyword_wrapper, kw) for kw in keywords]
        for future in as_completed(futures):
            results = future.result()
            if results:
                all_results.extend(results)

    if all_results:
        keys = all_results[0].keys()
        with open(OUTPUT_FILE, "w", newline="", encoding='utf-8') as f:
            writer = csv.DictWriter(f, keys)
            writer.writeheader()
            writer.writerows(all_results)

        print(f"\n✅ Scraping complete! Total rows: {len(all_results)} saved to {OUTPUT_FILE}")
    else:
        print("⚠️ No data scraped. Check keywords or blocks.")

if __name__ == "__main__":
    main()
