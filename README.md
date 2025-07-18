

# 🧴 Digital Shelf Visibility & Pricing Intelligence — Skincare (Amazon, Flipkart, Nykaa, Myntra)

This project tracks and visualizes **daily product rank, discount %, and stock status** across 3 major marketplaces for the **skincare category** using web scraping + Tableau dashboard.

## 🔧 Tools Used
- Python + Playwright (scraping)
- Pandas (data wrangling)
- Tableau Public (dashboard) - https://shorturl.at/s1hK4
- Google Sheets (auto-sync) - https://shorturl.at/GOwt6
  
## 📌 Business Problem
"How often does my brand appear in the **Top 10 search results** for important skincare keywords — and at what price, rank, and stock level — compared to competitors?"

## 📊 Key Insights
1. **Deep Discounts ≠ High Rank** — SEO matters more  
2. **Stockouts Hurt Visibility** — OOS SKUs often drop from Top 5  
3. **Top Brands Vary by Platform** — Lakme, Minimalist, Bare Anatomy dominate

## 📁 Repo Structure
/Scraper/ → Python scraping and cleaning code
/data/ → Sample scraped data
/visuals/ → Tableau dashboard snapshots
/tableau_dashboard/ → Dashboard PDF or .twbx file
README.md → This file


## 🖥️ Dashboard Snapshot

![Dashboard](visuals/dashboard_screenshot1.png)

## 🚀 How to Run
1. Install dependencies: `pip install -r requirements.txt`
2. Run `scrape_amazon.py` to scrape data
3. Use Tableau to open `.twbx` file or analyze the sample dataset

## 📬 Contact
For collaboration, feedback, or walkthrough:  
devikarajadurai@gmail.com | https://www.linkedin.com/in/rajadhurai-t-988a6835b/
