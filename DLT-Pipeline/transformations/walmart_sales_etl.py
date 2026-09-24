from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date, sum, avg, desc

# BRONZE LAYER: Raw Data Ingestion

@dp.table(
    name="walmart_bronze",
    comment="Bronze table: Raw Walmart sales dataset ingested directly from the workspace"
)
def walmart_raw():
    # Reading as a stream is best practice for a DLT to process new data incrementally
    return spark.readStream.table("workspace.default.walmart_dataset")

# SILVER LAYER: Cleansed & Prepared Data

@dp.table(
    name="walmart_silver",
    comment="Silver table: Cleansed Walmart data with proper data types and quality checks"
)
# DLT Expectations (Data Quality Checks)
@dp.expect_or_drop("valid_sales", "Weekly_Sales > 0") # Drop rows with negative/zero sales
@dp.expect("valid_store_id", "Store IS NOT NULL") # Flag rows missing a Store ID
def walmart_prepared():
    return (
        spark.readStream.table("walmart_bronze")
        # The date in the sample is DD/MM/YY format, so we cast it to an actual DateType
        .withColumn("Date", to_date(col("Date"), "dd/MM/yy"))
        # Selecting only the business relevant columns, leaving behind raw metadata like bronze_id
        .select(
            "Store",
            "Date",
            "Weekly_Sales",
            "Holiday_Flag",
            "Temperature",
            "CPI",
            "Unemployment",
            "temp_category"
        )
    )

# GOLD LAYER: Business-Level Aggregations

# Creating a summarized view that a BI tool or business analyst would use.
@dp.table(
    name="walmart_gold_store_performance",
    comment="Gold Table: Aggregated total sales and average temperature per store by weather category."
)
def store_sales_summary():
    # Gold tables often read as static batches rather than streams to compute full aggregations
    return (
        spark.read.table("walmart_silver")
        .groupBy("Store", "temp_category")
        .agg(
            sum("Weekly_Sales").alias("Total_Sales"),
            avg("Temperature").alias("Avg_Temperature")
        )
        # Sorting the output to the highest performing stores at the top
        .sort(desc("Total_Sales"))
    )

