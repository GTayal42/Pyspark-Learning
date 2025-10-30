import pandas as pd
import Levenshtein
from tqdm import tqdm

tqdm.pandas()  # progress bar for pandas apply

# -----------------------
# CONFIG
# -----------------------
input_file = "sample_data.csv"
output_file = "deduped_output.csv"
similarity_threshold = 0.8  # 80%

# -----------------------
# LOAD DATA
# -----------------------
print("📥 Reading CSV file...")
df = pd.read_csv(input_file)

# Fill NaN values
df = df.fillna("")

# Add helper column
df["ConcatString"] = (
    df["FullName"].astype(str)
    + df["FirstName"].astype(str)
    + df["MiddleName"].astype(str)
    + df["LastName"].astype(str)
    + df["City"].astype(str)
    + df["State"].astype(str)
    + df["Zip"].astype(str)
    + df["Address1"].astype(str)
)

# Initialize columns
df["Status"] = "Active"
df["GroupID"] = None

# -----------------------
# GROUPING LOGIC
# -----------------------
def group_key(row):
    """Assign record to a group based on name/address similarity buckets."""
    return (
        row["FullName"][:5].lower(),  # Same FullName group
        (row["FirstName"] + row["LastName"])[:5].lower(),  # Same FN+LN group
        (str(row["Address1"]) + str(row["City"]) + str(row["State"]) + str(row["Zip"]))[:5].lower()
    )

df["GroupKey"] = df.apply(group_key, axis=1)

# -----------------------
# DEDUPLICATION
# -----------------------
print("🔍 Performing fuzzy matching within groups...")

group_counter = 1
final_records = []

for key, group in tqdm(df.groupby("GroupKey")):
    group = group.copy()
    n = len(group)
    matched = set()

    if n == 1:
        group["GroupID"] = f"G{group_counter:05d}"
        final_records.append(group)
        group_counter += 1
        continue

    for i in range(n):
        if i in matched:
            continue
        base = group.iloc[i]
        base_concat = base["ConcatString"]

        group_id = f"G{group_counter:05d}"
        group.at[base.name, "GroupID"] = group_id
        matched.add(i)

        for j in range(i + 1, n):
            if j in matched:
                continue
            compare = group.iloc[j]
            compare_concat = compare["ConcatString"]

            sim = Levenshtein.ratio(base_concat, compare_concat)
            if sim >= similarity_threshold:
                # Choose active by longer FullName
                if len(compare["FullName"]) > len(base["FullName"]):
                    group.at[base.name, "Status"] = "Inactive"
                    group.at[compare.name, "Status"] = "Active"
                else:
                    group.at[compare.name, "Status"] = "Inactive"

                group.at[compare.name, "GroupID"] = group_id
                matched.add(j)

        group_counter += 1
    final_records.append(group)

# -----------------------
# FINAL OUTPUT
# -----------------------
final_df = pd.concat(final_records, ignore_index=True)
final_df = final_df.drop(columns=["ConcatString", "GroupKey"])

# Save to CSV
final_df.to_csv(output_file, index=False, encoding="utf-8-sig")
print(f"✅ Deduplication complete. Output saved to: {output_file}")