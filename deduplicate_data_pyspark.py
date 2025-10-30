from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat_ws, lit, length, levenshtein,
    when, row_number, monotonically_increasing_id, substring
)
from pyspark.sql.window import Window

# ==========================================
# 1️⃣ Spark Initialization
# ==========================================
spark = SparkSession.builder \
    .appName("Fuzzy Deduplication 50L") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 2️⃣ Load the data
# ==========================================
input_file = "sample_data.csv"
output_path = "output_dedup_50L"

df = spark.read.csv(input_file, header=True, inferSchema=True)
df = df.fillna("")

# Add Unique ID for reference
df = df.withColumn("RowID", monotonically_increasing_id())

# ==========================================
# 3️⃣ Create Group Key to reduce comparisons
# ==========================================
df = df.withColumn(
    "GroupKey",
    concat_ws("_",
        substring(col("FirstName"), 1, 3),
        substring(col("LastName"), 1, 3),
        substring(col("City"), 1, 3)
    )
)

# ==========================================
# 4️⃣ Create Concatenated String for Matching
# ==========================================
df = df.withColumn("ConcatString",
                   concat_ws("|", *[col(c) for c in ["FullName","FirstName","MiddleName","LastName","Address1","City","State","Zip"]])
)

# ==========================================
# 5️⃣ Self-Join within GroupKey
# ==========================================
df1 = df.alias("a")
df2 = df.alias("b")

# Inner join on GroupKey to compare only similar prefixes
pairs = df1.join(df2, on="GroupKey", how="inner") \
    .where(col("a.RowID") < col("b.RowID"))  # avoid self and reverse duplicates

# ==========================================
# 6️⃣ Compute Similarity using Levenshtein distance
# ==========================================
pairs = pairs.withColumn(
    "Similarity",
    (1 - (levenshtein(col("a.ConcatString"), col("b.ConcatString")) /
           length(col("a.ConcatString"))))
)

# ==========================================
# 7️⃣ Mark duplicates (Threshold = 0.8)
# ==========================================
pairs = pairs.withColumn("IsDuplicate", when(col("Similarity") > 0.8, lit(1)).otherwise(lit(0)))

dupes = pairs.filter(col("IsDuplicate") == 1)

# ==========================================
# 8️⃣ Decide Active vs Inactive
#     Longer FullName wins (more detailed record)
# ==========================================
dupes = dupes.withColumn(
    "ActiveRecord",
    when(length(col("a.FullName")) >= length(col("b.FullName")), col("a.RowID")).otherwise(col("b.RowID"))
)

dupes = dupes.withColumn(
    "InactiveRecord",
    when(length(col("a.FullName")) < length(col("b.FullName")), col("a.RowID")).otherwise(col("b.RowID"))
)

# ==========================================
# 9️⃣ Prepare final Active/Inactive status table
# ==========================================
active_ids = dupes.select(col("ActiveRecord").alias("RowID")).distinct().withColumn("Status", lit("Active"))
inactive_ids = dupes.select(col("InactiveRecord").alias("RowID")).distinct().withColumn("Status", lit("Inactive"))

# Union all statuses
status_df = active_ids.union(inactive_ids).distinct()

# ==========================================
# 🔟 Merge with original data
# ==========================================
final_df = df.join(status_df, on="RowID", how="left") \
    .fillna({"Status": "Active"})  # Records not marked duplicate remain Active

# Add a UniqueGroupID (same for similar records)
window = Window.partitionBy("GroupKey").orderBy("RowID")
final_df = final_df.withColumn("UniqueGroupID", row_number().over(window))

# ==========================================
# 11️⃣ Save Final Output
# ==========================================
final_df.select("UniqueGroupID","RowID","FullName","FirstName","LastName","Address1","City","State","Zip","Status") \
    .write.mode("overwrite").csv(output_path + "_csv", header=True)

final_df.write.mode("overwrite").parquet(output_path + "_parquet")

# ==========================================
# ✅ Summary
# ==========================================
print("✅ Deduplication complete!")
print(f"Active Records: {final_df.filter(col('Status') == 'Active').count()}")
print(f"Inactive Records: {final_df.filter(col('Status') == 'Inactive').count()}")

spark.stop()