"""
real    0m33.000s
user    0m41.973s
sys     0m6.124s
2 executors

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode
from pyspark.sql.functions import col, count, datediff, lit, sqrt, pow, explode
import pandas as pd
import matplotlib.pyplot as plt

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # Minimize log output

# Paths to datasets
categories_path = "/user/s2810832/data/categories/categories.zstd.parquet"
places_path = "/user/s2810832/data/places/places-00000.zstd.parquet"

# Read datasets
cat_df = spark.read.parquet(categories_path)
pl_df = spark.read.parquet(places_path)

# Filter data for specified countries and non-null closures
countries = ["DE", "FR", "IT", "PL", "ES", "TR"]
filtered_pois = pl_df.filter(
    (col("country").isin(countries)) & (col("date_closed").isNotNull())
)

# Explode the `fsq_category_ids` array into individual rows
exploded_pois = filtered_pois.withColumn("fsq_category_id", explode(col("fsq_category_ids")))

# Join with categories dataset to get category names
poi_with_categories = exploded_pois.join(
    cat_df,
    exploded_pois["fsq_category_id"] == cat_df["category_id"],
    "left"
)

# Group by category name and count closures
category_closures = poi_with_categories.groupBy("category_name").agg(
    count("fsq_place_id").alias("closure_count")
)

# Convert Spark DataFrame to Pandas for printing and plotting
category_closures_pd = category_closures.orderBy(
    col("closure_count").desc()
).limit(10).toPandas()  # Updated limit to 10

# Print the top 10 categories
print("Top 10 Categories with Most Closures Across DE, FR, IT, PL, ES, TR:")
print(category_closures_pd)

# Ensure only 10 categories are plotted
top_10_categories = category_closures_pd.head(10)

# Plot a single bar chart for the top 10 categories
plt.figure(figsize=(12, 8))
plt.bar(top_10_categories["category_name"], top_10_categories["closure_count"])
plt.title("Top 10 Categories with Most Closures Across DE, FR, IT, PL, ES, TR")
plt.xlabel("Category")
plt.ylabel("Number of Closures")
plt.xticks(rotation=45)
plt.tight_layout()

# Save the chart as a PNG file
output_file = "top_10_categories_closures.png"
plt.savefig(output_file)
print(f"Chart saved as {output_file}")
plt.close()

# Read datasets
cat_df = spark.read.parquet(categories_path)
pl_df = spark.read.parquet(places_path)

# Filter for specified countries and non-null closures
countries = ["DE", "FR", "IT", "PL", "ES", "TR"]
filtered_pois = pl_df.filter(
    (col("country").isin(countries)) & (col("date_closed").isNotNull())
)

# Explode the `fsq_category_ids` array into individual rows for restaurants
exploded_pois = filtered_pois.withColumn("fsq_category_id", explode(col("fsq_category_ids")))

# Join with categories to get category names for restaurants
restaurants_with_categories = exploded_pois.join(
    cat_df, exploded_pois["fsq_category_id"] == cat_df["category_id"], "left"
).filter(col("category_name") == "Restaurant")

# Join with categories to add category names for POIs
pois_with_categories = pl_df.withColumn("fsq_category_id", explode(col("fsq_category_ids"))).join(
    cat_df, col("fsq_category_id") == cat_df["category_id"], "left"
)

# Find nearby POIs within 2 km
def haversine_distance(lat1, lon1, lat2, lon2):
    return 111.32 * sqrt(pow(lat1 - lat2, 2) + pow(lon1 - lon2, 2))

nearby_pois = restaurants_with_categories.alias("rest").crossJoin(
    pois_with_categories.alias("poi")
).withColumn(
    "distance",
    haversine_distance(
        col("rest.latitude"), col("rest.longitude"),
        col("poi.latitude"), col("poi.longitude")
    )
).filter((col("distance") <= 2.0) & (col("rest.fsq_place_id") != col("poi.fsq_place_id")))  # Updated range to 2 km

# Analyze the impact on nearby POIs
impact_analysis = nearby_pois.withColumn(
    "days_after_closure",
    datediff(col("poi.date_closed"), col("rest.date_closed"))
).filter(col("days_after_closure") >= 0)

# Group by POI category name and count closures
impact_by_category = impact_analysis.groupBy("poi.category_name").agg(
    count("poi.fsq_place_id").alias("closure_count")
)

# Convert Spark DataFrame to Pandas for visualization
impact_by_category_pd = impact_by_category.orderBy(
    col("closure_count").desc()
).toPandas()

# Limit to the top 20 categories for the chart
top_10_categories = impact_by_category_pd.head(10)

# Plot a bar chart of the impact for top 10 categories
plt.figure(figsize=(12, 8))
plt.bar(top_10_categories["category_name"], top_10_categories["closure_count"])
plt.title("Impact of Restaurant Closures on Nearby POIs (2 km) across DE, FR, IT, PL, ES, TR")
plt.xlabel("Category")
plt.ylabel("Number of Nearby Closures")
plt.xticks(rotation=45)
plt.tight_layout()

# Save the chart
output_file = "restaurant_closure_impact_2km.png"
plt.savefig(output_file)
print(f"Chart saved as {output_file}")
plt.close()
