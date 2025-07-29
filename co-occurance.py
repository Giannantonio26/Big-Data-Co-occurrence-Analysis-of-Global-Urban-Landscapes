#Execution time: 42m with 2 executors

import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lower, lit, sqrt, pow

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # Set log level to ERROR to minimize log output

# Paths to datasets
categories_path = "/user/s2810832/data/categories/categories.zstd.parquet"
places_path = "/user/s2810832/data/places/places-00000.zstd.parquet"

# Read datasets
cat_df = spark.read.parquet(categories_path)
pl_df = spark.read.parquet(places_path)

# Explode the category IDs in the Places Dataset
pl_exploded_df = pl_df.withColumn("fsq_category_id", explode(col("fsq_category_ids")))

# Join the Places Dataset with the Categories Dataset
combined_df = pl_exploded_df.join(
    cat_df,
    pl_exploded_df["fsq_category_id"] == cat_df["category_id"],
    "left"
)

# Create a filter expression for relevant categories
name_filter = lower(col("category_name")).like("%grocer%") | \
              lower(col("category_name")).like("%supermarket%") | \
              lower(col("category_name")).like("%food store%")

# Filter for supermarkets
supermarkets = combined_df.filter(name_filter).select(
    "fsq_place_id", "name", "country", "latitude", "longitude", "category_name"
)

# Get the top 20 countries by the number of supermarkets
top_countries = supermarkets.groupBy("country").count().orderBy(col("count").desc()).limit(20)

# Collect top countries into a list
top_countries_list = top_countries.collect()

# Print the number of supermarkets for each top-20 country
print("Top 20 Countries with the Most Supermarkets:")
print("-" * 40)
print(f"{'Country':<15} {'Number of Supermarkets':<20}")
print("-" * 40)
for row in top_countries_list:
    print(f"{row['country']:<15} {row['count']:<20}")

# Filter supermarkets in the top-10 countries only
top_10_countries = top_countries_list[:10]
top_10_country_names = [row["country"] for row in top_10_countries]

supermarkets_in_top_10_countries = supermarkets.filter(
    col("country").isin(top_10_country_names)
)

# Define a function to calculate nearby categories
def calculate_nearby_categories(supermarket, all_places):
    lat, lon = supermarket["latitude"], supermarket["longitude"]
    nearby_places = all_places.withColumn(
        "distance",
        sqrt(pow(col("latitude") - lit(lat), 2) + pow(col("longitude") - lit(lon), 2)) * 111.32
    ).filter(col("distance") <= 1.0)  # 1-km range

    # Count the categories for nearby places
    category_counts = nearby_places.groupBy("category_name").count().orderBy(col("count").desc())
    return category_counts.collect()

# Dictionary to store category occurrences for the top 10 countries
country_category_dict = {}

# Process only the top 10 countries
for country_row in top_10_countries:
    country = country_row["country"]
    country_category_dict[country] = {}

    # Get supermarkets for this country
    country_supermarkets = supermarkets_in_top_10_countries.filter(col("country") == country).collect()

    for supermarket in country_supermarkets:
        # Find nearby categories
        nearby_categories = calculate_nearby_categories(supermarket, combined_df)

        # Update the dictionary for this country
        for row in nearby_categories:
            category = row["category_name"]
            count = row["count"]
            if category in country_category_dict[country]:
                country_category_dict[country][category] += count
            else:
                country_category_dict[country][category] = count

    # Save each country's dictionary as a Pickle file
    file_name = f"{country}_categories_1km.pkl"
    with open(file_name, "wb") as pickle_file:
        pickle.dump(country_category_dict[country], pickle_file)
        print(f"Saved dictionary for {country} to {file_name}")

# Print the dictionary for the top 10 countries
for country, category_dict in country_category_dict.items():
    print(f"\nCategory Counts for {country}:")
    print("-" * 40)
    for category, count in category_dict.items():
        # Replace None with "Unknown" for category names
        category = category if category is not None else "Unknown"
        print(f"{category:<30} {count:<10}")
