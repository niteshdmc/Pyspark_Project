import csv
import os
import random
from datetime import datetime, timedelta

from resources.dev import config
from src.main.upload.upload_to_s3 import UploadToS3, s3_client

customer_ids = list(range(1, 21))
store_ids = list(range(121, 124))
product_data = {
    # Electronics
    "Apple iPhone 14 Pro": 134999,
    "Sony WH-1000XM5 Headphones": 29990,
    "Samsung 55-inch 4K QLED TV": 84990,
    "Dell XPS 13 Laptop": 124999,
    "Apple Watch Series 9": 45900,


    # Kitchen & Appliances
    "Prestige Induction Cooktop": 4990,
    "Dyson V12 Vacuum Cleaner": 65900,

    # Fashion & Accessories
    "Ray-Ban Aviator Sunglasses": 9990,
    "Tissot PRX Watch": 49500,
    "Nike Air Max Sneakers": 12999,

    # Beauty & Personal Care
    "Clinique Moisture Surge Cream": 2700,
    "Dyson Supersonic Hair Dryer": 32900,

    # Premium Groceries & Beverages
    "Amul 85% Dark Chocolate": 450,
    "India Gate Basmati Rice": 700,
    "Davidoff Coffee Beans": 1100,

    # Health & Fitness
    "Powermax Adjustable Dumbbell Set": 14999,
    "Cosco Resistance Band": 350
}

sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9]
}

start_date = datetime(2023, 3, 3)
end_date = datetime(2023, 8, 20)

file_location = "Z:\\Projects\\relmart-project\\rawdata\\upload_to_s3"
csv_file_path = os.path.join(file_location, "sales_data.csv")
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost"])

    for _ in range(500):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = price * quantity

        csvwriter.writerow([customer_id, store_id, product_name, sales_date.strftime("%Y-%m-%d"), sales_person_id, price, quantity, total_cost])

print("CSV file generated successfully.")

s3_prefix = config.s3_source_s3prefix
s3_bucket = config.bucket_name
local_path = config.from_local_to_s3

uploader = UploadToS3(s3_client)
result = uploader.upload_to_s3(s3_bucket,s3_prefix,local_path)
