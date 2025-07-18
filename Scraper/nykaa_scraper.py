import pandas as pd
import time, csv, os
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor

KEYWORDS_FILE = "data/keywords.csv"
OUTPUT_FILE = "output/nykaa_output.csv"


def scrape_single_keyword(keyword):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US"
        )

        # Block images, fonts, media to speed up
        context.route("**/*", lambda route, request: route.abort() if request.resource_type in ["image", "media",
                                                                                                "font"] else route.continue_())

        page = context.new_page()
        search_url = f"https://www.nykaa.com/search/result/?q={keyword.replace(' ', '%20')}"
        print(f"\n🔍 Searching: {search_url}")

        try:
            page.goto(search_url, timeout=60000, wait_until="domcontentloaded")
            time.sleep(3)
        except Exception as e:
            print(f"❌ Failed to load page for: {keyword} — {e}")
            browser.close()
            return []

        results = []
        product_cards = page.locator("a.css-qlopj4")
        count = product_cards.count()

        for i in range(min(10, count)):
            card = product_cards.nth(i)

            # 🔍 Detect Sponsored label
            try:
                parent = card.locator("..").locator("..")
                ad_tag = parent.locator("li.custom-tag:has-text('AD')")
                listing_type = "Sponsored" if ad_tag.count() > 0 else "Organic"
            except:
                listing_type = "Organic"

            try:
                title = card.locator("div.css-xrzmfa").inner_text(timeout=2000)
            except:
                title = "N/A"

            try:
                price = card.locator("span.css-111z9ua").inner_text().replace("₹", "").replace(",", "").strip()
                price = float(price)
            except:
                price = None

            try:
                mrp = card.locator("span.css-17x46n5 span").inner_text().replace("₹", "").replace(",", "").strip()
                mrp = float(mrp)
            except:
                mrp = price

            try:
                discount_percent = round(((mrp - price) / mrp) * 100, 2) if mrp and price else None
            except:
                discount_percent = None

            try:
                partial_link = card.get_attribute("href")
                full_link = f"https://www.nykaa.com{partial_link}" if partial_link else "N/A"
            except:
                full_link = "N/A"

            stock_status = "In Stock" if price else "Out of Stock"

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

        browser.close()
        return results


def main():
    os.makedirs("output", exist_ok=True)

    try:
        df = pd.read_csv(KEYWORDS_FILE)
        keywords = df["keyword"].dropna().tolist()
    except Exception as e:
        print(f"❌ Error reading keyword file: {e}")
        return

    all_results = []

    # 🧵 Run 4 threads in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_kw = {executor.submit(scrape_single_keyword, kw): kw for kw in keywords}
        for future in future_to_kw:
            kw = future_to_kw[future]
            try:
                result = future.result()
                all_results.extend(result)
            except Exception as e:
                print(f"❌ Failed to scrape {kw}: {e}")

    if all_results:
        keys = all_results[0].keys()
        with open(OUTPUT_FILE, "w", newline="", encoding='utf-8') as f:
            writer = csv.DictWriter(f, keys)
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\n✅ Scraping complete. Data saved to {OUTPUT_FILE}")
    else:
        print("⚠️ No data collected.")


if __name__ == "__main__":
    main()
