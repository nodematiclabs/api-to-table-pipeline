import requests

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("HackerNewsTopPostsToBigQuery") \
    .getOrCreate()

# Capture the current time as the pipeline run time
pipeline_run_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

# Function to fetch top posts from Hacker News
def fetch_top_posts():
    top_stories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    story_ids = requests.get(top_stories_url).json()
    top_posts = []

    for rank, story_id in enumerate(story_ids[:32], start=1):  # Top 32 posts
        story_url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
        story_data = requests.get(story_url).json()
        top_posts.append((pipeline_run_time, story_data['id'], story_data['title'], rank))

    return top_posts

# Fetch data
top_posts_data = fetch_top_posts()

# Create DataFrame
df = spark.createDataFrame(top_posts_data, ["datetime", "id", "title", "rank"])

# Convert 'datetime' to a timestamp type
df = df.withColumn("datetime", df["datetime"].cast("timestamp"))
# Optionally add more transformation logic

# BigQuery settings
bq_output_dataset = "hacker_news"
bq_output_table = "top_stories"
bq_temp_bucket = "hacker-news-top-stories-demo"

# Write to BigQuery
df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", bq_temp_bucket) \
    .option("table", f"{bq_output_dataset}.{bq_output_table}") \
    .mode("append") \
    .save()

# Stop the Spark Session
spark.stop()
