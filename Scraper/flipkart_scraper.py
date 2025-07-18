import pandas as pd
from playwright.sync_api import sync_playwright
import time
import csv
import os
import traceback

KEYWORDS_FILE = "data/keywords.csv"
OUTPUT_FILE = "output/flipkart_output.csv"


def scrape_flipkart_search_results(keyword):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        search_url = f"https://www.flipkart.com/search?q={keyword.replace(' ', '+')}"
        print(f"\n🔍 Scraping Flipkart: {keyword}")
        print(f"🔗 Visiting: {search_url}")
        page.goto(search_url, timeout=60000)
        page.wait_for_load_state("networkidle")
        time.sleep(3)

        try:
            page.locator("button._2KpZ6l._2doB4z").click(timeout=3000)
        except:
            pass  # No popup

        product_cards = page.locator("div._75nlfW > div")
        count = product_cards.count()

        if count == 0:
            print(f"⚠️ No items found for '{keyword}' — structure may have changed.")
            page.screenshot(path=f"output/{keyword}_flipkart_debug.png", full_page=True)
            browser.close()
            return []

        results = []

        for i in range(min(10, count)):
            item = product_cards.nth(i)

            try:
                title = item.locator("a.wjcEIp").inner_text(timeout=3000).strip()
            except:
                title = "N/A"

            try:
                price_text = item.locator("div.Nx9bqj").inner_text()
                price = float(price_text.replace("₹", "").replace(",", "").strip())
            except:
                price = None

            try:
                mrp_text = item.locator("div.yRaY8j").inner_text()
                mrp = float(mrp_text.replace("₹", "").replace(",", "").strip())
            except:
                mrp = price  # fallback

            try:
                discount_text = item.locator("div.UkUFwK span").inner_text()
                discount_percent = discount_text.strip()
            except:
                discount_percent = "N/A"

            try:
                link = item.locator("a.VJA3rP").get_attribute("href")
                full_link = f"https://www.flipkart.com{link}" if link else "N/A"
            except:
                full_link = "N/A"

            stock_status = "In Stock" if price else "Out of Stock"

            # ✅ Detect sponsored listings
            try:
                if item.locator("div.xgS27m").count() > 0:
                    listing_type = "Sponsored"
                else:
                    listing_type = "Organic"
            except:
                listing_type = "Organic"

            results.append({
                "keyword": keyword,
                "rank": i + 1,
                "listing_type": listing_type,
                "product_name": title,
                "price": price if price else "N/A",
                "mrp": mrp if mrp else "N/A",
                "discount_percent": discount_percent,
                "stock_status": stock_status,
                "url": full_link
            })

        browser.close()
        return results


def main():
    os.makedirs("output", exist_ok=True)
    try:
        df = pd.read_csv(KEYWORDS_FILE)
    except Exception as e:
        print(f"❌ Could not read keywords file: {e}")
        return

    all_results = []

    for kw in df['keyword']:
        try:
            results = scrape_flipkart_search_results(kw)
            all_results.extend(results)
        except Exception as e:
            print(f"❌ Error scraping '{kw}': {e}")
            traceback.print_exc()

    if all_results:
        keys = all_results[0].keys()
        with open(OUTPUT_FILE, "w", newline="", encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, keys)
            dict_writer.writeheader()
            dict_writer.writerows(all_results)

        print(f"\n✅ Flipkart scraping complete! Data saved to {OUTPUT_FILE}")
    else:
        print("⚠️ No data scraped from Flipkart. Check keywords or page structure.")


if __name__ == "__main__":
    main()
