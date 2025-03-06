import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.function import col, lower, when

spark = SparkSession.builder.appName("PokemonEbayAnalytics").getOrCreate()

BASE_URL = ("https://www.ebay.co.uk/sch/i.html?_nkw=pokemon+prismatic+evolutions+%22elite+trainer+box%22+%22etb%22"
            "&_sacat=0&_from=R40&LH_Sold=2&LH_Complete=1&_ipg=240&rt=nc&_udhi=250")


def scrape_ebay_pages(base_url, max_pages=0):
    all_results = []  # List to store all results

    for page in range(1, max_pages + 1):
        url = f"{base_url}&_pgn={page}"
        print(f"Scraping page {page}: {url}")

        # Fetch the page
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        # Find the results container
        results = soup.find(id="srp-river-results")
        profile_cards = results.find_all("div", class_="s-item__info clearfix") if results else []

        # Extract data for each card
        for card in profile_cards:
            date_sold = card.find("div", class_="s-item__caption")
            title_text = card.find("div", class_="s-item__title")
            price_text = card.find("div", class_="s-item__detail s-item__detail--primary")
            if date_sold and title_text and price_text:
                # Clean date_sold text
                date_cleaned = date_sold.text.replace(" Sold ", "").replace(" Sold", "").strip()

                # Clean price_text by removing the pound sign and any leading/trailing spaces

                price_cleaned = price_text.text.replace("£", "").strip()

                # Append to results
                all_results.append([date_cleaned, title_text.text.strip(), price_cleaned])

    return all_results
