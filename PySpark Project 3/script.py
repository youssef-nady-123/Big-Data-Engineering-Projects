"""
================================================================================
 COFFEE SHOP SALES ANALYTICS — END-TO-END PYSPARK PROJECT
================================================================================
Dataset : Coffee_Shop_Dataset.csv  (~149,000 transaction-level rows)
Author  : Youssef  (Big Data Engineering — interview-prep project)

WHAT THIS SCRIPT DOES
----------------------
This script builds a small but complete PySpark data pipeline on top of a
retail coffee-shop transactions dataset, following the MEDALLION
ARCHITECTURE pattern that is standard in modern Lakehouse projects:

    BRONZE  -> raw data, ingested as-is, schema enforced, nothing dropped
    SILVER  -> cleaned, de-duplicated, correctly typed, business rules applied
    GOLD    -> analytics-ready STAR SCHEMA (dim_date, dim_store, dim_product,
               fact_sales) + a set of business analytics built with window
               functions, joins (including a broadcast join) and aggregations

Every section is heavily commented on purpose: this file is meant to be
read line-by-line as interview-preparation material for PySpark / Big Data
engineering interviews, so the comments explain *what* the code does and
*why* it is written that way (which PySpark API is used, why a broadcast
join is safe here, why we repartition before writing, etc.).

HOW TO RUN
----------
    spark-submit script.py
    (or simply: python3 script.py — a local SparkSession is created below)

OUTPUTS
-------
    outputs/gold/dim_date/         -> partitioned Parquet dimension table
    outputs/gold/dim_store/        -> Parquet dimension table
    outputs/gold/dim_product/      -> Parquet dimension table
    outputs/gold/fact_sales/       -> Parquet fact table, partitioned by month
    outputs/charts/*.png           -> matplotlib charts for each analysis
    outputs/summaries/*.csv        -> small aggregated result tables (1 file each)
================================================================================
"""

# ------------------------------------------------------------------------
# 0. IMPORTS
# ------------------------------------------------------------------------
# pyspark.sql.SparkSession is the entry point to every Spark application.
# pyspark.sql.functions gives us the vectorized, Catalyst-optimized column
# expressions (col, when, sum, avg, window functions, date functions ...).
# pyspark.sql.window.Window is used to build OVER(PARTITION BY ... ORDER BY
# ...) style window specifications, exactly like in SQL.
# pyspark.sql.types lets us define an explicit schema instead of relying on
# (slow and sometimes wrong) schema inference.
# ------------------------------------------------------------------------
import os
import shutil

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, DateType
)

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # headless backend - we only save PNGs, never show()
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# ------------------------------------------------------------------------
# Project-wide constants (paths). Keeping them at the top makes the script
# portable: change these three lines and the whole pipeline runs elsewhere.
# ------------------------------------------------------------------------
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
RAW_CSV_PATH  = os.path.join(BASE_DIR, "data", "coffee_shop.csv")
GOLD_DIR      = os.path.join(BASE_DIR, "outputs", "gold")
CHARTS_DIR    = os.path.join(BASE_DIR, "outputs", "charts")
SUMMARY_DIR   = os.path.join(BASE_DIR, "outputs", "summaries")

os.makedirs(CHARTS_DIR, exist_ok=True)
os.makedirs(SUMMARY_DIR, exist_ok=True)

# A small, consistent color palette used across every chart so the whole
# project (script + notebook + PDF) has one visual identity.
COLORS = {
    "coffee_dark":  "#4B2E2B",
    "coffee":       "#6F4E37",
    "coffee_light": "#A9746E",
    "cream":        "#E8DCCA",
    "gold":         "#C89B3C",
    "green":        "#5B8266",
    "red":          "#B33A3A",
}
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor":   "white",
    "axes.edgecolor":   COLORS["coffee_dark"],
    "axes.labelcolor":  COLORS["coffee_dark"],
    "xtick.color":      COLORS["coffee_dark"],
    "ytick.color":      COLORS["coffee_dark"],
    "text.color":       COLORS["coffee_dark"],
    "font.size":        10,
})


# ------------------------------------------------------------------------
# 1. SPARK SESSION
# ------------------------------------------------------------------------
# .master("local[*]")  -> run locally using all available cores (fine for a
#   ~150K row dataset; on a real cluster this line would be removed and the
#   master would be supplied by spark-submit / YARN / Kubernetes instead).
# spark.sql.shuffle.partitions is lowered from the 200 default because our
#   dataset is small — 200 tiny shuffle partitions would just add overhead.
# ------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("CoffeeShopSalesAnalytics")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")  # keep console output readable

print("=" * 80)
print("STEP 1 — Spark session created:", spark.version)
print("=" * 80)


# ============================================================================
# 2. BRONZE LAYER — raw ingestion
# ============================================================================
# We define an EXPLICIT schema instead of using inferSchema=True.
# Why: schema inference forces Spark to do an extra full (or sampled) pass
# over the file just to guess types, and it can guess wrong (e.g. it might
# read "Total Bill" as a string if a value looks odd). Declaring the schema
# up front is faster and safer — this is standard practice in production
# ingestion jobs.
# ----------------------------------------------------------------------------
bronze_schema = StructType([
    StructField("transaction_id",   IntegerType(), True),
    StructField("transaction_date", StringType(),  True),  # parsed in SILVER
    StructField("transaction_time", StringType(),  True),  # parsed in SILVER
    StructField("store_id",         IntegerType(), True),
    StructField("store_location",   StringType(),  True),
    StructField("product_id",       IntegerType(), True),
    StructField("transaction_qty",  IntegerType(), True),
    StructField("unit_price",       DoubleType(),  True),
    StructField("product_category", StringType(),  True),
    StructField("product_type",     StringType(),  True),
    StructField("product_detail",   StringType(),  True),
    StructField("Size",             StringType(),  True),
    StructField("Total Bill",       DoubleType(),  True),
    StructField("Month Name",       StringType(),  True),
    StructField("Day Name",         StringType(),  True),
    StructField("Hour",             IntegerType(), True),
    StructField("Day of Week",      IntegerType(), True),
    StructField("Month",            IntegerType(), True),
])

bronze_df = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .schema(bronze_schema)
    .csv(RAW_CSV_PATH)
)

bronze_count = bronze_df.count()
print(f"\nSTEP 2 — BRONZE: loaded {bronze_count:,} raw rows")
bronze_df.printSchema()


# ============================================================================
# 3. SILVER LAYER — cleaning, typing, business rules
# ============================================================================
# Cleaning steps applied here:
#   a) Rename columns with spaces to snake_case (Spark supports spaces in
#      column names, but they are painful to work with in SQL expressions).
#   b) Parse the string date "6/1/2023" and string time "11:33:29 AM" into
#      a proper TimestampType using to_date / to_timestamp with explicit
#      format patterns — never rely on default parsing for real projects.
#   c) Trim whitespace from every string column (common real-world issue).
#   d) Drop exact duplicate rows (dropDuplicates on the natural key).
#   e) Filter out structurally invalid rows (non-positive qty/price) —
#      a very small number of such rows, if any, would corrupt aggregates.
#   f) Recompute total_amount ourselves (qty * unit_price) instead of
#      trusting the source "Total Bill" column, then keep both so we can
#      cross-check them — this is a classic silver-layer data-quality check.
# ----------------------------------------------------------------------------
silver_df = (
    bronze_df
    .withColumnRenamed("Total Bill", "total_bill_source")
    .withColumnRenamed("Size", "size")
    .withColumnRenamed("Month Name", "month_name")
    .withColumnRenamed("Day Name", "day_name")
    .withColumnRenamed("Hour", "hour_of_day")
    .withColumnRenamed("Day of Week", "day_of_week")
    .withColumnRenamed("Month", "month_num")
    # trim every string column in one pass
    .withColumn("store_location",   F.trim(F.col("store_location")))
    .withColumn("product_category", F.trim(F.col("product_category")))
    .withColumn("product_type",     F.trim(F.col("product_type")))
    .withColumn("product_detail",   F.trim(F.col("product_detail")))
    .withColumn("size",             F.trim(F.col("size")))
    # proper date/timestamp parsing from the raw US-style strings
    .withColumn("transaction_date", F.to_date(F.col("transaction_date"), "M/d/yyyy"))
    .withColumn("transaction_ts",
                F.to_timestamp(
                    F.concat_ws(" ", F.col("transaction_date").cast("string"),
                                F.col("transaction_time")),
                    "yyyy-MM-dd h:mm:ss a"))
    # data-quality check: recompute revenue ourselves rather than trusting
    # the source column, then flag any mismatch (should be ~0 rows here)
    .withColumn("total_amount",
                F.round(F.col("transaction_qty") * F.col("unit_price"), 2))
    .withColumn("revenue_mismatch_flag",
                F.abs(F.col("total_amount") - F.col("total_bill_source")) > 0.01)
    .drop("total_bill_source")
    # keep only structurally valid transactions
    .filter((F.col("transaction_qty") > 0) & (F.col("unit_price") > 0))
    .dropDuplicates(["transaction_id"])
)

silver_df.cache()  # reused by every downstream step below
silver_count = silver_df.count()
mismatches = silver_df.filter(F.col("revenue_mismatch_flag")).count()

print(f"\nSTEP 3 — SILVER: {silver_count:,} clean rows "
      f"({bronze_count - silver_count:,} dropped as duplicates/invalid)")
print(f"           revenue cross-check mismatches: {mismatches}")


# ============================================================================
# 4. GOLD LAYER — STAR SCHEMA
# ============================================================================
# We model the cleaned data as a classic STAR SCHEMA:
#
#     dim_date  ----\
#     dim_store -----> fact_sales <----- dim_product
#
# fact_sales holds one row per transaction line with foreign keys into the
# three dimension tables plus the numeric measures (qty, unit_price,
# total_amount). This is the same modeling pattern used across the other
# interview-prep material (SQL Server version of this same dataset), kept
# consistent here in PySpark.
# ----------------------------------------------------------------------------

# --- dim_date --------------------------------------------------------------
# Built from the DISTINCT dates present in the data plus calendar
# attributes we will need for time-series analytics (month, day name,
# weekend flag). date_id is an integer surrogate key in yyyymmdd form,
# which is a common, sort-friendly convention for date dimensions.
dim_date = (
    silver_df
    .select("transaction_date", "month_name", "day_name",
            "day_of_week", "month_num")
    .distinct()
    .withColumn("date_id", F.date_format("transaction_date", "yyyyMMdd").cast("int"))
    .withColumn("quarter", F.quarter("transaction_date"))
    .withColumn("is_weekend", F.col("day_name").isin("Saturday", "Sunday"))
    .select("date_id", F.col("transaction_date").alias("full_date"),
            "day_name", "day_of_week", "month_name", "month_num",
            "quarter", "is_weekend")
    .orderBy("date_id")
)

# --- dim_store ---------------------------------------------------------------
dim_store = (
    silver_df
    .select("store_id", "store_location")
    .distinct()
    .orderBy("store_id")
)

# --- dim_product -------------------------------------------------------------
dim_product = (
    silver_df
    .select("product_id", "product_category", "product_type",
            "product_detail", "size", "unit_price")
    .distinct()
    .orderBy("product_id")
)

# --- fact_sales ----------------------------------------------------------
# One row per original transaction line, joined back to date_id so it can
# be linked to dim_date. store_id / product_id are already the surrogate
# (and natural, in this dataset) keys shared with the dimension tables, so
# no extra join is needed for those two.
fact_sales = (
    silver_df
    .withColumn("date_id", F.date_format("transaction_date", "yyyyMMdd").cast("int"))
    .select(
        "transaction_id", "date_id", "store_id", "product_id",
        "hour_of_day", "transaction_qty", "unit_price", "total_amount"
    )
)

print("\nSTEP 4 — GOLD (star schema) row counts:")
print(f"  dim_date    : {dim_date.count():,} rows")
print(f"  dim_store   : {dim_store.count():,} rows")
print(f"  dim_product : {dim_product.count():,} rows")
print(f"  fact_sales  : {fact_sales.count():,} rows")

# Persist the star schema as Parquet — columnar, compressed, splittable,
# and the standard format for a gold layer. fact_sales is partitioned by
# month_num pulled from dim_date via a broadcast join (see section 5a) so
# BI tools / downstream Spark jobs can prune partitions on month filters.
fact_with_month = (
    fact_sales.join(F.broadcast(dim_date.select("date_id", "month_num")),
                     on="date_id", how="left")
)

for path in [os.path.join(GOLD_DIR, p) for p in
             ["dim_date", "dim_store", "dim_product", "fact_sales"]]:
    if os.path.exists(path):
        shutil.rmtree(path)

dim_date.write.mode("overwrite").parquet(os.path.join(GOLD_DIR, "dim_date"))
dim_store.write.mode("overwrite").parquet(os.path.join(GOLD_DIR, "dim_store"))
dim_product.write.mode("overwrite").parquet(os.path.join(GOLD_DIR, "dim_product"))
(fact_with_month
 .write.mode("overwrite")
 .partitionBy("month_num")
 .parquet(os.path.join(GOLD_DIR, "fact_sales")))

print("           star schema written to outputs/gold/ as Parquet "
      "(fact_sales partitioned by month_num)")


# ============================================================================
# 5. ANALYTICS — window functions, joins, aggregations
# ============================================================================
# Every query below builds on top of the star schema using a BROADCAST JOIN
# for the small dimension tables (a handful to a few thousand rows) against
# the larger fact table. Broadcasting avoids an expensive shuffle: Spark
# ships a full copy of the small table to every executor instead of
# shuffling the huge fact table across the network — the standard
# optimization for "big fact, small dimension" joins.
# ----------------------------------------------------------------------------
sales = (
    fact_with_month
    .drop("month_num")  # will come back in via dim_date below - avoid a duplicate column
    .join(F.broadcast(dim_store),   on="store_id",   how="left")
    .join(F.broadcast(dim_product), on="product_id", how="left")
    .join(F.broadcast(dim_date),    on="date_id",     how="left")
)
sales.cache()
sales.count()  # materialize the cache


def save_chart(fig, filename):
    """Small helper: save a matplotlib figure to CHARTS_DIR with tight
    layout and a consistent dpi, then close it (headless backend)."""
    fig.tight_layout()
    fig.savefig(os.path.join(CHARTS_DIR, filename), dpi=150,
                facecolor="white")
    plt.close(fig)


def save_summary(pdf: pd.DataFrame, filename):
    """Small helper: write a small aggregated pandas result to CSV so the
    numbers behind every chart are reviewable outside Spark/Jupyter too."""
    pdf.to_csv(os.path.join(SUMMARY_DIR, filename), index=False)


# ----------------------------------------------------------------------
# 5a. Revenue by store & month + a RUNNING CUMULATIVE TOTAL per store
#     using a window function with an unbounded-preceding frame.
# ----------------------------------------------------------------------
store_month_revenue = (
    sales.groupBy("store_id", "store_location", "month_num", "month_name")
    .agg(F.round(F.sum("total_amount"), 2).alias("monthly_revenue"))
)

running_window = (
    Window.partitionBy("store_id")
    .orderBy("month_num")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
store_month_revenue = store_month_revenue.withColumn(
    "running_cumulative_revenue",
    F.round(F.sum("monthly_revenue").over(running_window), 2)
).orderBy("store_id", "month_num")

print("\n--- 5a. Revenue by store/month with running cumulative total ---")
store_month_revenue.show(12, truncate=False)
save_summary(store_month_revenue.toPandas(), "store_month_revenue.csv")

# Chart: one line per store showing cumulative revenue growth across months
pdf_smr = store_month_revenue.toPandas().sort_values(["store_location", "month_num"])
fig, ax = plt.subplots(figsize=(8, 5))
palette = [COLORS["coffee_dark"], COLORS["coffee"], COLORS["gold"]]
for i, (loc, grp) in enumerate(pdf_smr.groupby("store_location")):
    ax.plot(grp["month_name"], grp["running_cumulative_revenue"],
            marker="o", label=loc, color=palette[i % len(palette)], linewidth=2)
ax.set_title("Cumulative Revenue by Store Over Time", fontweight="bold")
ax.set_ylabel("Cumulative revenue ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
ax.legend(title="Store")
save_chart(fig, "01_cumulative_revenue_by_store.png")


# ----------------------------------------------------------------------
# 5b. Month-over-month revenue growth % using LAG()
# ----------------------------------------------------------------------
total_by_month = (
    sales.groupBy("month_num", "month_name")
    .agg(F.round(F.sum("total_amount"), 2).alias("total_revenue"))
    .orderBy("month_num")
)
mom_window = Window.orderBy("month_num")
total_by_month = (
    total_by_month
    .withColumn("prev_month_revenue", F.lag("total_revenue").over(mom_window))
    .withColumn(
        "mom_growth_pct",
        F.round(
            (F.col("total_revenue") - F.col("prev_month_revenue"))
            / F.col("prev_month_revenue") * 100, 2
        )
    )
)
print("\n--- 5b. Month-over-month growth % ---")
total_by_month.show(truncate=False)
save_summary(total_by_month.toPandas(), "month_over_month_growth.csv")

pdf_mom = total_by_month.toPandas()
fig, ax1 = plt.subplots(figsize=(8, 5))
ax1.bar(pdf_mom["month_name"], pdf_mom["total_revenue"], color=COLORS["cream"],
        edgecolor=COLORS["coffee_dark"], label="Revenue")
ax1.set_ylabel("Total revenue ($)")
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
ax2 = ax1.twinx()
ax2.plot(pdf_mom["month_name"], pdf_mom["mom_growth_pct"], color=COLORS["red"],
         marker="o", linewidth=2, label="MoM growth %")
ax2.set_ylabel("MoM growth (%)")
ax1.set_title("Monthly Revenue and Month-over-Month Growth", fontweight="bold")
fig.legend(loc="upper left", bbox_to_anchor=(0.12, 0.88))
save_chart(fig, "02_month_over_month_growth.png")


# ----------------------------------------------------------------------
# 5c. Top-3 products PER CATEGORY by revenue, using DENSE_RANK()
# ----------------------------------------------------------------------
category_product_revenue = (
    sales.groupBy("product_category", "product_type")
    .agg(F.round(F.sum("total_amount"), 2).alias("revenue"),
         F.sum("transaction_qty").alias("units_sold"))
)
rank_window = Window.partitionBy("product_category").orderBy(F.desc("revenue"))
top_products_per_category = (
    category_product_revenue
    .withColumn("rank_in_category", F.dense_rank().over(rank_window))
    .filter(F.col("rank_in_category") <= 3)
    .orderBy("product_category", "rank_in_category")
)
print("\n--- 5c. Top-3 product types per category (DENSE_RANK) ---")
top_products_per_category.show(30, truncate=False)
save_summary(top_products_per_category.toPandas(), "top_products_per_category.csv")


# ----------------------------------------------------------------------
# 5d. Store performance ranking: revenue, avg basket size, RANK()/ROW_NUMBER()
# ----------------------------------------------------------------------
store_performance = (
    sales.groupBy("store_id", "store_location")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.avg("total_amount"), 2).alias("avg_basket_value"),
    )
    .withColumn("revenue_rank", F.rank().over(Window.orderBy(F.desc("total_revenue"))))
    .orderBy("revenue_rank")
)
print("\n--- 5d. Store performance ranking ---")
store_performance.show(truncate=False)
save_summary(store_performance.toPandas(), "store_performance.csv")

pdf_store = store_performance.toPandas()
fig, ax = plt.subplots(figsize=(7, 5))
bars = ax.bar(pdf_store["store_location"], pdf_store["total_revenue"],
              color=[COLORS["coffee_dark"], COLORS["coffee"], COLORS["gold"]])
ax.set_title("Total Revenue by Store", fontweight="bold")
ax.set_ylabel("Total revenue ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
for b in bars:
    ax.annotate(f"${b.get_height():,.0f}", (b.get_x() + b.get_width() / 2, b.get_height()),
                ha="center", va="bottom", fontsize=9)
save_chart(fig, "03_revenue_by_store.png")


# ----------------------------------------------------------------------
# 5e. Hour-of-day and day-of-week demand patterns (peak-time analysis)
# ----------------------------------------------------------------------
hourly_pattern = (
    sales.groupBy("hour_of_day")
    .agg(F.count("transaction_id").alias("num_transactions"),
         F.round(F.sum("total_amount"), 2).alias("revenue"))
    .orderBy("hour_of_day")
)
print("\n--- 5e. Hourly demand pattern ---")
hourly_pattern.show(24, truncate=False)
save_summary(hourly_pattern.toPandas(), "hourly_pattern.csv")

pdf_hour = hourly_pattern.toPandas()
fig, ax = plt.subplots(figsize=(9, 5))
ax.bar(pdf_hour["hour_of_day"], pdf_hour["num_transactions"], color=COLORS["coffee"])
ax.set_title("Transactions by Hour of Day", fontweight="bold")
ax.set_xlabel("Hour (24h)")
ax.set_ylabel("Number of transactions")
ax.set_xticks(pdf_hour["hour_of_day"])
save_chart(fig, "04_hourly_transaction_pattern.png")

# day-of-week ordered Mon..Sun rather than alphabetically
day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
dow_pattern = (
    sales.groupBy("day_name")
    .agg(F.round(F.sum("total_amount"), 2).alias("revenue"))
)
pdf_dow = dow_pattern.toPandas().set_index("day_name").reindex(day_order).reset_index()
save_summary(pdf_dow, "day_of_week_pattern.csv")

fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(pdf_dow["day_name"], pdf_dow["revenue"], color=COLORS["coffee_light"])
ax.set_title("Revenue by Day of Week", fontweight="bold")
ax.set_ylabel("Revenue ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
save_chart(fig, "05_revenue_by_day_of_week.png")


# ----------------------------------------------------------------------
# 5f. Category contribution to total revenue (% of whole)
# ----------------------------------------------------------------------
category_share = (
    sales.groupBy("product_category")
    .agg(F.round(F.sum("total_amount"), 2).alias("revenue"))
)
total_revenue_all = category_share.agg(F.sum("revenue")).collect()[0][0]
category_share = (
    category_share
    .withColumn("pct_of_total", F.round(F.col("revenue") / F.lit(total_revenue_all) * 100, 2))
    .orderBy(F.desc("revenue"))
)
print("\n--- 5f. Revenue contribution by product category ---")
category_share.show(truncate=False)
save_summary(category_share.toPandas(), "category_share.csv")

pdf_cat = category_share.toPandas().sort_values("revenue")
fig, ax = plt.subplots(figsize=(8, 6))
ax.barh(pdf_cat["product_category"], pdf_cat["pct_of_total"], color=COLORS["gold"])
ax.set_title("Revenue Share by Product Category", fontweight="bold")
ax.set_xlabel("% of total revenue")
save_chart(fig, "06_category_revenue_share.png")


# ----------------------------------------------------------------------
# 5g. Size preference (Small/Medium/Large) within each category
# ----------------------------------------------------------------------
size_mix = (
    sales.filter(F.col("size").isNotNull())
    .groupBy("product_category", "size")
    .agg(F.sum("transaction_qty").alias("units_sold"))
)
cat_totals = size_mix.groupBy("product_category").agg(F.sum("units_sold").alias("cat_total"))
size_mix = (
    size_mix.join(F.broadcast(cat_totals), on="product_category")
    .withColumn("pct_within_category", F.round(F.col("units_sold") / F.col("cat_total") * 100, 1))
    .orderBy("product_category", F.desc("units_sold"))
)
print("\n--- 5g. Size mix within each category ---")
size_mix.show(30, truncate=False)
save_summary(size_mix.toPandas(), "size_mix.csv")


# ============================================================================
# 6. WRAP-UP
# ============================================================================
print("\n" + "=" * 80)
print("PIPELINE COMPLETE")
print(f"  Gold star-schema tables : {GOLD_DIR}")
print(f"  Charts (PNG)            : {CHARTS_DIR}")
print(f"  Summary tables (CSV)    : {SUMMARY_DIR}")
print("=" * 80)

silver_df.unpersist()
sales.unpersist()
spark.stop()
