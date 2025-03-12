import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when


spark = SparkSession.builder.appName("EBayAnalyticsTest").getOrCreate()


BASE_URL = (
    #"https://www.ebay.co.uk/sch/i.html?_from=R40&_nkw=pokemon+journey+together&_sacat=0&LH_Complete=1&LH_Sold=2&_ipg"
    #"=240"
    "https://www.ebay.co.uk/sch/i.html?_nkw=pokemon+prismatic+evolutions+%22elite+trainer+box%22+%22etb%22&_sacat=0&_from=R40&LH_Sold=2&LH_Complete=1&_ipg=240&rt=nc&_udhi=250"
)


def scrape_ebay_pages(base_url, max_pages=0):
    all_results = []  # List to store all results

    for page in range(1, max_pages + 1):
        # Append pagination parameter
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


# Scrape data from all pages
max_pages = 8  # Adjust the number of pages as needed
data = scrape_ebay_pages(BASE_URL, max_pages=max_pages)


df = spark.createDataFrame(data, schema=["date_sold", "title_text", "price_text"])

# Remove the word 'Sold' and convert to date
from pyspark.sql.functions import col, regexp_replace, to_date, trim

# Set Spark to use legacy date/time parsing
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Remove 'Sold' and extra spaces, then convert to date
df = df.withColumn("date_sold", trim(regexp_replace(col("date_sold"), "Sold", "")))
df = df.withColumn("date_sold", to_date(col("date_sold"), "dd MMM yyyy"))

print("total sales: ")
print(len(data))

print("How many sold by today: ")
groupby = df.groupBy(["date_sold"]).count()
groupby.orderBy(col("date_sold").desc()).show()
# Add category column based on conditions
df = df.withColumn(
    "category",
    when(
        (lower(col("title_text")).contains("pokemon center")) |
        (lower(col("title_text")).contains("pc exclusive")) |
        (lower(col("title_text")).contains("pokémon center")) |
        (lower(col("title_text")).contains("pc")),
        "PCETB"
    )
    .when(
        (lower(col("title_text")).contains("booster box")) |
        (lower(col("title_text")).contains("booster display box")) |
        (lower(col("title_text")).contains("36")),
        "Booster Box"
    )
    .when(
        (lower(col("title_text")).contains("booster bundle")) &
        ~(lower(col("title_text")).contains("booster bundles")),  # Exclude "booster bundles"
        "Booster Bundle"
    )
    .when(
        (lower(col("title_text")).contains("Surprise Box")),
        "Surprise Box"
    )
    .when(
        (lower(col("title_text")).contains("Surprise Box")),
        "Surprise Box"
    )
    .when(
        (lower(col("title_text")).contains("Booster Pack")) |
        (lower(col("title_text")).contains("Booster Packs")),
        "Surprise Box"
    )
    .when(
        (lower(col("title_text")).contains("Binder")),
        "Binder Collection"
    )
    .when(
        (lower(col("title_text")).contains("Tech Sticker")),
        "Tech Sticker Collection"
    )
    .when(
        (lower(col("title_text")).contains("/")) |
        (lower(col("title_text")).contains("Holo")) |
        (lower(col("title_text")).contains("SVP-173")) |
        (lower(col("title_text")).contains("SVP173")) |
        (lower(col("title_text")).contains("Pokeball")) |
        (lower(col("title_text")).contains("Poke ball")) |
        (lower(col("title_text")).contains("sir")) |
        (lower(col("title_text")).contains("Stamped Promo")) |
        (lower(col("title_text")).contains("SIR")) |
        (lower(col("title_text")).contains("EX")) |
        (lower(col("title_text")).contains("ex")),
        "Pokemon Card"
    )
    .otherwise("Normal ETB")
)

df.show()

# Group by category and date_sold, count occurrences
grouped_df = df.groupBy("category", "date_sold").count().orderBy(col("date_sold").desc())

# Order by date_sold in descending order
ordered_df = grouped_df.orderBy(col("date_sold").desc())

# Show results
print("Grouped Data:")
ordered_df.show(3000, truncate=False)

print("Total Sales by Catageory")
cata = df.groupBy(["category"]).count()
cata.show()

print("Average price of each category: ")
# Convert 'price_text' to a numeric column
df = df.withColumn("price", df["price_text"].cast("double"))

# Group by 'category' and calculate the average price
avg_cata = df.groupBy("category").agg({"price": "avg"})

# Rename the column for clarity
avg_cata = avg_cata.withColumnRenamed("avg(price)", "average_price")

avg_cata.show()

# Filter and show titles in "Normal ETB" category
print("Normal ETB Titles:")
others_df = df.filter(col("category") == "Normal ETB")
others_df.select("title_text").show(20, truncate=False)
