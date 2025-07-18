import pandas as pd
import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright
import random

KEYWORDS_FILE = "data/keywords.csv"
OUTPUT_FILE = "output/myntra_output.csv"
MAX_THREADS = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/116 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) Chrome/113 Safari/537.36",
]

def scrape_myntra_search_results(keyword):
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale='en-IN',
            viewport={"width": 1280, "height": 800},
            timezone_id="Asia/Kolkata"
        )
        page = context.new_page()

        search_url = f"https://www.myntra.com/{keyword.replace(' ', '-')}?rawQuery={keyword.replace(' ', '%20')}"
        print(f"\n🔍 Searching Myntra for: {keyword}")
        print(f"🔗 URL: {search_url}")

        try:
            page.goto(search_url, timeout=30000, wait_until='load')
            page.wait_for_timeout(random.randint(1500, 2500))

            product_cards = page.locator("li.product-base")
            count = product_cards.count()
            print(f"🧾 Found {count} products.")

            for i in range(min(10, count)):
                try:
                    card = product_cards.nth(i)

                    title = "N/A"
                    if card.locator("h3.product-brand").count() > 0:
                        title = card.locator("h3.product-brand").inner_text(timeout=1000).strip()

                    product_name = "N/A"
                    if card.locator("h4.product-product").count() > 0:
                        product_name = card.locator("h4.product-product").inner_text(timeout=1000).strip()

                    full_name = f"{title} {product_name}".strip()

                    price = None
                    if card.locator("span.product-discountedPrice").count() > 0:
                        price_text = card.locator("span.product-discountedPrice").inner_text(timeout=1000)
                        price = float(price_text.replace("Rs.", "").replace(",", "").strip())

                    mrp = price
                    if card.locator("span.product-strike").count() > 0:
                        mrp_text = card.locator("span.product-strike").inner_text(timeout=1000)
                        mrp = float(mrp_text.replace("Rs.", "").replace(",", "").strip())

                    discount_percent = "N/A"
                    if card.locator("span.product-discountPercentage").count() > 0:
                        discount_percent = card.locator("span.product-discountPercentage").inner_text(timeout=1000).strip()

                    rating = "N/A"
                    if card.locator("div.product-ratingsContainer > span").count() > 0:
                        rating = card.locator("div.product-ratingsContainer > span").first.inner_text(timeout=1000)

                    reviews = "N/A"
                    if card.locator("div.product-ratingsCount").count() > 0:
                        reviews = card.locator("div.product-ratingsCount").inner_text(timeout=1000).replace("|", "").strip()

                    listing_type = "Organic"
                    if card.locator("div.product-waterMark").count() > 0:
                        ad_label = card.locator("div.product-waterMark").inner_text(timeout=1000)
                        if "AD" in ad_label.upper():
                            listing_type = "Sponsored"

                    link = None
                    if card.locator("a").count() > 0:
                        link = card.locator("a").get_attribute("href", timeout=1000)
                    full_link = f"https://www.myntra.com{link}" if link else "N/A"

                    stock_status = "In Stock" if price else "Out of Stock"

                    print(f"📦 [{i+1}] {full_name[:50]}... | ₹{price} → ₹{mrp} | {discount_percent} | {listing_type}")

                    results.append({
                        "keyword": keyword,
                        "rank": i + 1,
                        "listing_type": listing_type,
                        "product_name": full_name,
                        "price": price if price else "N/A",
                        "mrp": mrp if mrp else "N/A",
                        "discount_percent": discount_percent,
                        "rating": rating,
                        "review_count": reviews,
                        "stock_status": stock_status,
                        "url": full_link
                    })

                except Exception as e:
                    print(f"⚠️ Skipping product #{i+1} due to error: {e}")
                    continue

        except Exception as e:
            print(f"❌ Failed to scrape '{keyword}': {e}")
        finally:
            browser.close()

    return results

def main():
    os.makedirs("output", exist_ok=True)

    try:
        df = pd.read_csv(KEYWORDS_FILE)
        keywords = df.iloc[:, 0].dropna().tolist()
    except Exception as e:
        print(f"❌ Failed to read keywords file: {e}")
        return

    all_results = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(scrape_myntra_search_results, kw) for kw in keywords]
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_results.extend(result)

    if all_results:
        keys = all_results[0].keys()
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\n✅ Done. Saved to {OUTPUT_FILE}")
    else:
        print("⚠️ No data scraped.")

if __name__ == "__main__":
    main()
