# ==============================================================================
# PROJECT   : Employee Sales Analysis with PySpark
# DATASET   : employee_sales.csv (emp_id, name, department, region, sales_amount, sale_date)
# GOAL      : Clean the raw sales data, then produce a set of business-ready
#             aggregations: totals by department/region, top performers,
#             monthly trends, and employee rankings.
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, round as _round,
    to_date, date_format, dense_rank, rank
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)
from pyspark.sql.window import Window


# ------------------------------------------------------------------------------
# STEP 1: Create the SparkSession
# ------------------------------------------------------------------------------
# This is the entry point to any PySpark program. "local[*]" tells Spark to
# run locally using all available CPU cores instead of connecting to a cluster.
spark = SparkSession.builder \
    .appName("EmployeeSalesAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# Reduce noisy log output so only warnings/errors show in the console.
spark.sparkContext.setLogLevel("WARN")


# ------------------------------------------------------------------------------
# STEP 2: Define an explicit schema
# ------------------------------------------------------------------------------
# We define the schema ourselves instead of using inferSchema=True. This is
# faster (Spark doesn't have to scan the file twice to guess types) and safer
# (we control exactly what type each column becomes).
sales_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("region", StringType(), True),
    StructField("sales_amount", DoubleType(), True),
    StructField("sale_date", StringType(), True),   # read as string first, parse next
])


# ------------------------------------------------------------------------------
# STEP 3: Read the raw CSV file
# ------------------------------------------------------------------------------
raw_df = spark.read.csv(
    "data/employee_sales.csv",
    header=True,
    schema=sales_schema
)

print("Raw data sample:")
raw_df.show(5)
print(f"Total rows loaded: {raw_df.count()}")


# ------------------------------------------------------------------------------
# STEP 4: Clean and transform the data
# ------------------------------------------------------------------------------
# The sale_date column arrives as text like "1/5/2024" (M/D/YYYY). We convert
# it into a real DateType column so we can group by month/year later.
clean_df = raw_df.withColumn(
    "sale_date", to_date(col("sale_date"), "M/d/yyyy")
)

# Add a "sale_month" column (e.g. "2024-01") used for monthly trend reporting.
clean_df = clean_df.withColumn(
    "sale_month", date_format(col("sale_date"), "yyyy-MM")
)

# Drop any row that failed to parse into a valid date or is missing key fields.
clean_df = clean_df.dropna(subset=["emp_id", "sales_amount", "sale_date"])

# Cache the cleaned DataFrame since we reuse it across multiple aggregations.
clean_df.cache()

print("\nCleaned data sample:")
clean_df.show(5)


# ------------------------------------------------------------------------------
# STEP 5: Total and average sales per department
# ------------------------------------------------------------------------------
dept_summary = clean_df.groupBy("department") \
    .agg(
        _round(_sum("sales_amount"), 2).alias("total_sales"),
        _round(avg("sales_amount"), 2).alias("avg_sale"),
        count("emp_id").alias("num_transactions")
    ) \
    .orderBy(col("total_sales").desc())

print("\nSales summary by department:")
dept_summary.show()


# ------------------------------------------------------------------------------
# STEP 6: Total sales per region
# ------------------------------------------------------------------------------
region_summary = clean_df.groupBy("region") \
    .agg(
        _round(_sum("sales_amount"), 2).alias("total_sales"),
        count("emp_id").alias("num_transactions")
    ) \
    .orderBy(col("total_sales").desc())

print("\nSales summary by region:")
region_summary.show()


# ------------------------------------------------------------------------------
# STEP 7: Total sales per employee
# ------------------------------------------------------------------------------
employee_summary = clean_df.groupBy("emp_id", "name", "department") \
    .agg(
        _round(_sum("sales_amount"), 2).alias("total_sales"),
        count("emp_id").alias("num_transactions")
    ) \
    .orderBy(col("total_sales").desc())

print("\nSales summary by employee (top 10):")
employee_summary.show(10)


# ------------------------------------------------------------------------------
# STEP 8: Rank employees within each department (Window Function)
# ------------------------------------------------------------------------------
# A window function lets us rank rows within groups without collapsing them
# into a single aggregated row, unlike groupBy(). Here we rank employees by
# total sales, restarting the ranking at 1 for every department.
dept_window = Window.partitionBy("department").orderBy(col("total_sales").desc())

ranked_employees = employee_summary.withColumn(
    "rank_in_department", dense_rank().over(dept_window)
)

print("\nTop performer in each department:")
ranked_employees.filter(col("rank_in_department") == 1).show()


# ------------------------------------------------------------------------------
# STEP 9: Monthly sales trend
# ------------------------------------------------------------------------------
monthly_trend = clean_df.groupBy("sale_month") \
    .agg(_round(_sum("sales_amount"), 2).alias("total_sales")) \
    .orderBy("sale_month")

print("\nMonthly sales trend:")
monthly_trend.show(20)


# ------------------------------------------------------------------------------
# STEP 10: Write results to disk (Parquet, partitioned where useful)
# ------------------------------------------------------------------------------
dept_summary.write.mode("overwrite").parquet("output/dept_summary")
region_summary.write.mode("overwrite").parquet("output/region_summary")
employee_summary.write.mode("overwrite").parquet("output/employee_summary")
monthly_trend.write.mode("overwrite").parquet("output/monthly_trend")

# Also save the top-performer-per-department result as CSV for easy viewing.
ranked_employees.filter(col("rank_in_department") == 1) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", True) \
    .csv("output/top_performers")

print("\nAll results written to the 'output/' directory.")


# ------------------------------------------------------------------------------
# STEP 11: Stop the Spark session
# ------------------------------------------------------------------------------
# Always stop the SparkSession when the job finishes to release cluster/local
# resources cleanly.
spark.stop()
