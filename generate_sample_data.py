import csv
import random
from faker import Faker

fake = Faker()

num_records = 100000  # total rows
duplicate_percent = 0.1  # 10% duplicates
num_unique = int(num_records * (1 - duplicate_percent))

output_file = "sample_data.csv"
headers = ["FullName", "FirstName", "MiddleName", "LastName", "City", "State", "Zip", "Address1"]

def random_middle_name():
    """Return random middle name or empty."""
    return fake.first_name() if random.random() < 0.5 else ""

def shorten_name(name):
    """Return a shortened name (like Tayal -> T)."""
    return name[0] if name and random.random() < 0.7 else name

# Step 1: Generate base unique records
unique_records = []
for _ in range(num_unique):
    first = fake.first_name()
    middle = random_middle_name()
    last = fake.last_name()
    full = f"{first} {middle + ' ' if middle else ''}{last}"
    city = fake.city()
    state = fake.state()
    zipcode = fake.zipcode()
    address = fake.street_address()
    unique_records.append([full, first, middle, last, city, state, zipcode, address])

# Step 2: Create duplicates with small name variations
duplicate_records = []
for i in range(int(num_records - num_unique)):
    base = random.choice(unique_records)
    first, middle, last = base[1], base[2], base[3]
    # Introduce name variation
    varied_last = shorten_name(last)
    varied_middle = random.choice([middle, "", shorten_name(middle)])
    varied_full = f"{first} {varied_middle + ' ' if varied_middle else ''}{varied_last}"
    duplicate_records.append([
        varied_full, first, varied_middle, varied_last,
        base[4], base[5], base[6], base[7]  # same city, state, zip, address
    ])

# Step 3: Combine all and shuffle
all_records = unique_records + duplicate_records
random.shuffle(all_records)

# Step 4: Write to CSV
with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(all_records)

print(f"✅ {num_records} fake records (with duplicates) written to {output_file}")