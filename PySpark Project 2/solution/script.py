"""
================================================================================
 GOLD LAYER SALES ANALYTICS - END-TO-END PYSPARK DATAFRAME API PROJECT
================================================================================
 Author   : Yousseg
 Purpose  : Practice the full PySpark DataFrame API (Part 1 & Part 2 notes)
            on a real medallion "gold layer" star-schema dataset:
                - gold_fact_sales.csv     (fact table)
                - gold_dim_customers.csv  (dimension table)
                - gold_dim_products.csv   (dimension table)

 Sections in this script:
   1. SparkSession setup
   2. Schema definition + Data Reading (explicit StructType schemas)
   3. Data Exploration (printSchema, show, count, schema)
   4. Data Cleaning (handling NULLs, type casting)
   5. Column Transformations (select, alias, filter, withColumn, when/otherwise)
   6. Date Functions (year, month, datediff, delivery duration)
   7. String Functions (concat, upper, lower, trim, initcap)
   8. Joins (star-schema join: fact + both dimensions)
   9. Aggregations (groupBy, agg, count_distinct, collect_set)
  10. Window Functions (rank, dense_rank, row_number, lag, running total)
  11. Pivot Tables
  12. User-Defined Function (UDF)
  13. Writing results (partitioned Parquet)
  14. Spark SQL queries (temp views + SQL)
================================================================================
"""

# ==============================================================================
# 1. IMPORTS & SPARK SESSION SETUP
# ==============================================================================

from pyspark.sql import SparkSession                       # entry point to Spark
from pyspark.sql.types import (                            # explicit schema types
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType
)
from pyspark.sql.functions import (                        # transformation functions
    col, when, concat_ws, initcap, upper, lower, trim,
    year, month, datediff, current_date, round as spark_round,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    count_distinct, collect_set, row_number, rank, dense_rank,
    lag, lead, udf
)
from pyspark.sql.window import Window

# create (or get) a local SparkSession - the entry point for the DataFrame API
spark = SparkSession.builder \
    .appName("Gold_Layer_Sales_Analytics") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# reduce noisy logs so only warnings/errors are printed
spark.sparkContext.setLogLevel("WARN")

print("SparkSession created ->", spark.version)


# ==============================================================================
# 2. DEFINE SCHEMAS + READ THE THREE CSV FILES
# ==============================================================================
# We define the schema manually (StructType) instead of using inferSchema=True.
# This is faster (Spark doesn't scan the file twice) and guarantees the
# correct data types for every column, exactly as covered in Part 1
# ("Define Schema for Raw Data").

DATA_PATH = "data"   # folder containing the 3 csv files

# ---- 2.1 gold_dim_customers schema -------------------------------------------------
customers_schema = StructType([
    StructField("customer_key",    IntegerType(), True),
    StructField("customer_id",     IntegerType(), True),
    StructField("customer_number", StringType(),  True),
    StructField("first_name",      StringType(),  True),
    StructField("last_name",       StringType(),  True),
    StructField("country",         StringType(),  True),
    StructField("marital_status",  StringType(),  True),
    StructField("gender",          StringType(),  True),
    StructField("birthdate",       DateType(),    True),
    StructField("create_date",     DateType(),    True),
])

# ---- 2.2 gold_dim_products schema --------------------------------------------------
products_schema = StructType([
    StructField("product_key",    IntegerType(), True),
    StructField("product_id",     IntegerType(), True),
    StructField("product_number", StringType(),  True),
    StructField("product_name",   StringType(),  True),
    StructField("category_id",    StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("subcategory",    StringType(),  True),
    StructField("maintenance",    StringType(),  True),
    StructField("cost",           DoubleType(),  True),
    StructField("product_line",   StringType(),  True),
    StructField("start_date",     DateType(),    True),
])

# ---- 2.3 gold_fact_sales schema ----------------------------------------------------
sales_schema = StructType([
    StructField("order_number",   StringType(),  True),
    StructField("product_key",    IntegerType(), True),
    StructField("customer_key",   IntegerType(), True),
    StructField("order_date",     DateType(),    True),
    StructField("shipping_date",  DateType(),    True),
    StructField("due_date",       DateType(),    True),
    StructField("sales_amount",   DoubleType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("price",          DoubleType(),  True),
])

# ---- 2.4 Read the CSV files using the schemas --------------------------------------
# header=True  -> first row holds the column names
# schema=...   -> explicit schema instead of inferSchema (best practice)
df_customers_raw = spark.read.format("csv") \
    .option("header", True) \
    .schema(customers_schema) \
    .load(f"{DATA_PATH}/gold_dim_customers.csv")

df_products_raw = spark.read.format("csv") \
    .option("header", True) \
    .schema(products_schema) \
    .load(f"{DATA_PATH}/gold_dim_products.csv")

df_sales_raw = spark.read.format("csv") \
    .option("header", True) \
    .schema(sales_schema) \
    .load(f"{DATA_PATH}/gold_fact_sales.csv")

print("\n Row counts on read:")
print("customers:", df_customers_raw.count())
print("products :", df_products_raw.count())
print("sales    :", df_sales_raw.count())


# ==============================================================================
# 3. DATA EXPLORATION
# ==============================================================================

print("\n===== SCHEMA: customers =====")
df_customers_raw.printSchema()

print("\n===== SCHEMA: products =====")
df_products_raw.printSchema()

print("\n===== SCHEMA: sales =====")
df_sales_raw.printSchema()

print("\n===== SAMPLE: customers =====")
df_customers_raw.show(5)

print("\n===== SAMPLE: products =====")
df_products_raw.show(5)

print("\n===== SAMPLE: sales =====")
df_sales_raw.show(5)


# ==============================================================================
# 4. DATA CLEANING - HANDLING NULLS
# ==============================================================================
# Real quality issues found in the raw files:
#   customers.country  -> 337 nulls
#   customers.gender   -> 15 nulls
#   customers.birthdate-> 17 nulls
#   products.category / subcategory / maintenance -> 7 nulls each
#   products.product_line -> 17 nulls
#   sales.order_date   -> 19 nulls

print("\n===== NULL COUNTS BEFORE CLEANING =====")
df_customers_raw.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df_customers_raw.columns
]).show()

# fillna() replaces NULLs with a default value per-column (dict form)
df_customers_clean = df_customers_raw.fillna({
    "country": "Unknown",
    "gender": "Not Specified",
})

# drop rows where birthdate is null (can't safely guess a birthdate)
df_customers_clean = df_customers_clean.dropna(subset=["birthdate"])

df_products_clean = df_products_raw.fillna({
    "category": "Uncategorized",
    "subcategory": "Uncategorized",
    "maintenance": "Unknown",
    "product_line": "Unknown",
})

# sales: drop rows with a null order_date since it is a required business key
df_sales_clean = df_sales_raw.dropna(subset=["order_date"])

print("\n===== ROW COUNTS AFTER CLEANING =====")
print("customers:", df_customers_clean.count(), "(was", df_customers_raw.count(), ")")
print("products :", df_products_clean.count(),  "(was", df_products_raw.count(),  ")")
print("sales    :", df_sales_clean.count(),      "(was", df_sales_raw.count(),      ")")


# ==============================================================================
# 5. COLUMN TRANSFORMATIONS: select / alias / filter / withColumn / when-otherwise
# ==============================================================================

# ---- 5.1 select + alias: build a tidy customer name table --------------------------
df_customers_selected = df_customers_clean.select(
    col("customer_key"),
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("country"),
    col("gender"),
    col("birthdate").alias("date_of_birth"),
)

# ---- 5.2 filter: customers living in the United States -----------------------------
df_us_customers = df_customers_selected.filter(col("country") == "United States")
print("\nUS customers:", df_us_customers.count())

# ---- 5.3 withColumn + when/otherwise: classify customers by marital status flag ----
df_customers_flagged = df_customers_clean.withColumn(
    "is_married",
    when(col("marital_status") == "Married", "Yes").otherwise("No")
)

# ---- 5.4 withColumn + when/otherwise chain: product cost tier ---------------------
df_products_tiered = df_products_clean.withColumn(
    "cost_tier",
    when(col("cost") == 0, "Free/Component")
    .when(col("cost") < 500, "Low")
    .when(col("cost") < 2000, "Medium")
    .otherwise("High")
)

print("\n===== PRODUCT COST TIERS =====")
df_products_tiered.groupBy("cost_tier").count().show()

# ---- 5.5 type casting: ensure sales_amount / price are DoubleType (already are,
#          shown here for completeness using the .cast() transformation) ------------
df_sales_casted = df_sales_clean.withColumn(
    "sales_amount", col("sales_amount").cast(DoubleType())
).withColumn(
    "price", col("price").cast(DoubleType())
)


# ==============================================================================
# 6. DATE FUNCTIONS
# ==============================================================================
# extract order year/month, and compute delivery duration in days
# (datediff = shipping_date - order_date)

df_sales_dates = df_sales_casted.withColumn(
    "order_year", year(col("order_date"))
).withColumn(
    "order_month", month(col("order_date"))
).withColumn(
    "delivery_days", datediff(col("shipping_date"), col("order_date"))
).withColumn(
    "days_since_order", datediff(current_date(), col("order_date"))
)

print("\n===== SALES WITH DATE-DERIVED COLUMNS =====")
df_sales_dates.select(
    "order_number", "order_date", "shipping_date",
    "order_year", "order_month", "delivery_days"
).show(5)

print("\nAverage delivery time (days):")
df_sales_dates.select(spark_round(avg("delivery_days"), 2).alias("avg_delivery_days")).show()


# ==============================================================================
# 7. STRING FUNCTIONS
# ==============================================================================
# build a clean, presentable full_name column and standardize text case

df_customers_string = df_customers_flagged.withColumn(
    "full_name", initcap(concat_ws(" ", trim(col("first_name")), trim(col("last_name"))))
).withColumn(
    "country_upper", upper(col("country"))
).withColumn(
    "gender_lower", lower(col("gender"))
)

print("\n===== STRING TRANSFORMATIONS =====")
df_customers_string.select("first_name", "last_name", "full_name", "country_upper").show(5)


# ==============================================================================
# 8. JOINS - BUILDING THE STAR-SCHEMA (fact + both dimensions)
# ==============================================================================
# inner join: only sales rows with a matching customer AND a matching product

df_sales_full = df_sales_dates.join(
    df_customers_string, "customer_key", "inner"
).join(
    df_products_tiered, "product_key", "inner"
).select(
    df_sales_dates["order_number"],
    df_sales_dates["order_date"],
    df_sales_dates["order_year"],
    df_sales_dates["order_month"],
    df_sales_dates["delivery_days"],
    df_sales_dates["sales_amount"],
    df_sales_dates["quantity"],
    df_sales_dates["price"],
    df_customers_string["customer_key"],
    df_customers_string["full_name"],
    df_customers_string["country"],
    df_customers_string["gender"],
    df_customers_string["is_married"],
    df_products_tiered["product_key"],
    df_products_tiered["product_name"],
    df_products_tiered["category"],
    df_products_tiered["subcategory"],
    df_products_tiered["cost_tier"],
)

print("\n===== JOINED STAR-SCHEMA TABLE (sales + customers + products) =====")
df_sales_full.show(5)
print("Joined row count:", df_sales_full.count())

# ---- left_anti join example: products that were NEVER sold ------------------------
df_products_never_sold = df_products_tiered.join(
    df_sales_dates, "product_key", "left_anti"
).select("product_key", "product_name", "category")

print("\n===== PRODUCTS NEVER SOLD (left_anti join) =====")
df_products_never_sold.show(10, truncate=False)
print("Count of products never sold:", df_products_never_sold.count())


# ==============================================================================
# 9. AGGREGATIONS - groupBy().agg()
# ==============================================================================

# ---- 9.1 Revenue & orders per country ----------------------------------------------
df_revenue_by_country = df_sales_full.groupBy("country").agg(
    count("order_number").alias("total_orders"),
    spark_sum("sales_amount").alias("total_revenue"),
    spark_round(avg("sales_amount"), 2).alias("avg_order_value"),
    count_distinct("customer_key").alias("unique_customers"),
).orderBy(col("total_revenue").desc())

print("\n===== REVENUE BY COUNTRY =====")
df_revenue_by_country.show()

# ---- 9.2 Revenue by product category ------------------------------------------------
df_revenue_by_category = df_sales_full.groupBy("category").agg(
    spark_sum("quantity").alias("total_units_sold"),
    spark_sum("sales_amount").alias("total_revenue"),
    spark_round(avg("sales_amount"), 2).alias("avg_sale_amount"),
).orderBy(col("total_revenue").desc())

print("\n===== REVENUE BY PRODUCT CATEGORY =====")
df_revenue_by_category.show()

# ---- 9.3 collect_set: which categories has each customer purchased from -----------
df_customer_categories = df_sales_full.groupBy("customer_key", "full_name").agg(
    collect_set("category").alias("categories_purchased"),
    count("order_number").alias("total_orders")
).orderBy(col("total_orders").desc())

print("\n===== CATEGORIES PURCHASED PER CUSTOMER (collect_set) =====")
df_customer_categories.show(5, truncate=False)


# ==============================================================================
# 10. WINDOW FUNCTIONS
# ==============================================================================

# ---- 10.1 rank customers by total spend within their own country -------------------
customer_spend = df_sales_full.groupBy("country", "customer_key", "full_name").agg(
    spark_sum("sales_amount").alias("total_spent")
)

country_window = Window.partitionBy("country").orderBy(col("total_spent").desc())

df_customer_ranked = customer_spend.withColumn(
    "rank_in_country", rank().over(country_window)
).withColumn(
    "dense_rank_in_country", dense_rank().over(country_window)
).withColumn(
    "row_num", row_number().over(country_window)
)

print("\n===== TOP CUSTOMER PER COUNTRY (window rank) =====")
df_customer_ranked.filter(col("rank_in_country") == 1) \
    .orderBy(col("total_spent").desc()).show(10, truncate=False)

# ---- 10.2 running total of monthly revenue + month-over-month change (lag) --------
monthly_revenue = df_sales_full.groupBy("order_year", "order_month").agg(
    spark_sum("sales_amount").alias("monthly_revenue")
).orderBy("order_year", "order_month")

time_window = Window.orderBy("order_year", "order_month") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

monthly_lag_window = Window.orderBy("order_year", "order_month")

df_monthly_trend = monthly_revenue.withColumn(
    "running_total_revenue", spark_sum("monthly_revenue").over(time_window)
).withColumn(
    "previous_month_revenue", lag("monthly_revenue", 1).over(monthly_lag_window)
).withColumn(
    "next_month_revenue", lead("monthly_revenue", 1).over(monthly_lag_window)
)

print("\n===== MONTHLY REVENUE TREND (running total + lag/lead) =====")
df_monthly_trend.show(12)


# ==============================================================================
# 11. PIVOT TABLE
# ==============================================================================
# revenue by country (rows) x product category (columns)

df_pivot_country_category = df_sales_full.groupBy("country") \
    .pivot("category") \
    .agg(spark_round(spark_sum("sales_amount"), 2))

print("\n===== PIVOT: REVENUE BY COUNTRY x CATEGORY =====")
df_pivot_country_category.show(truncate=False)


# ==============================================================================
# 12. USER-DEFINED FUNCTION (UDF)
# ==============================================================================
# built-in functions can't cleanly express a custom loyalty-tier rule,
# so we register a plain python function as a UDF.

def loyalty_tier(total_spent):
    """Classify a customer into a loyalty tier based on total spend."""
    if total_spent is None:
        return "Unknown"
    if total_spent >= 5000:
        return "Platinum"
    elif total_spent >= 2000:
        return "Gold"
    elif total_spent >= 500:
        return "Silver"
    else:
        return "Bronze"

loyalty_udf = udf(loyalty_tier, StringType())

df_customer_loyalty = customer_spend.withColumn(
    "loyalty_tier", loyalty_udf(col("total_spent"))
)

print("\n===== CUSTOMER LOYALTY TIER (UDF) =====")
df_customer_loyalty.orderBy(col("total_spent").desc()).show(10, truncate=False)

print("\nLoyalty tier distribution:")
df_customer_loyalty.groupBy("loyalty_tier").count().orderBy(col("count").desc()).show()


# ==============================================================================
# 13. WRITE RESULTS - PARTITIONED PARQUET
# ==============================================================================
# the enriched fact table is written to Parquet, partitioned by order_year
# (a very common gold-layer output pattern)

output_path = "output/gold_sales_enriched_parquet"
df_sales_full.write.mode("overwrite").partitionBy("order_year").parquet(output_path)
print(f"\nEnriched sales table written to: {output_path} (partitioned by order_year)")


# ==============================================================================
# 14. SPARK SQL QUERIES
# ==============================================================================
# register DataFrames as temporary views so we can query them with plain SQL

df_sales_full.createOrReplaceTempView("vw_sales_full")
df_customers_string.createOrReplaceTempView("vw_customers")
df_products_tiered.createOrReplaceTempView("vw_products")
customer_spend.createOrReplaceTempView("vw_customer_spend")

print("\n===== SQL 1: Total revenue and orders per year =====")
spark.sql("""
    SELECT order_year,
           COUNT(order_number)            AS total_orders,
           ROUND(SUM(sales_amount), 2)    AS total_revenue
    FROM vw_sales_full
    GROUP BY order_year
    ORDER BY order_year
""").show()

print("\n===== SQL 2: Top 5 best-selling products by revenue =====")
spark.sql("""
    SELECT product_name,
           category,
           SUM(quantity)                 AS units_sold,
           ROUND(SUM(sales_amount), 2)   AS total_revenue
    FROM vw_sales_full
    GROUP BY product_name, category
    ORDER BY total_revenue DESC
    LIMIT 5
""").show(truncate=False)

print("\n===== SQL 3: Customers who spent above the overall average (subquery) =====")
spark.sql("""
    SELECT full_name, country, total_spent
    FROM vw_customer_spend
    WHERE total_spent > (SELECT AVG(total_spent) FROM vw_customer_spend)
    ORDER BY total_spent DESC
    LIMIT 10
""").show(truncate=False)

print("\n===== SQL 4: Ranking customers within each country using a SQL window function =====")
spark.sql("""
    SELECT country, full_name, total_spent,
           RANK() OVER (PARTITION BY country ORDER BY total_spent DESC) AS country_rank
    FROM vw_customer_spend
    QUALIFY country_rank <= 3
    ORDER BY country, country_rank
""").show(30, truncate=False)

print("\n===== SQL 5: Category revenue share of total revenue (CTE) =====")
spark.sql("""
    WITH category_totals AS (
        SELECT category, SUM(sales_amount) AS category_revenue
        FROM vw_sales_full
        GROUP BY category
    ),
    grand_total AS (
        SELECT SUM(sales_amount) AS overall_revenue FROM vw_sales_full
    )
    SELECT c.category,
           ROUND(c.category_revenue, 2)                                   AS category_revenue,
           ROUND(c.category_revenue * 100.0 / g.overall_revenue, 2)       AS pct_of_total
    FROM category_totals c
    CROSS JOIN grand_total g
    ORDER BY category_revenue DESC
""").show(truncate=False)

print("\n===== SQL 6: Products with above-average cost within their category (correlated subquery) =====")
spark.sql("""
    SELECT p1.product_name, p1.category, p1.cost
    FROM vw_products p1
    WHERE p1.cost > (
        SELECT AVG(p2.cost) FROM vw_products p2 WHERE p2.category = p1.category
    )
    ORDER BY p1.category, p1.cost DESC
""").show(15, truncate=False)


# ==============================================================================
# DONE
# ==============================================================================
print("\nProject finished successfully. Stopping SparkSession...")
spark.stop()
