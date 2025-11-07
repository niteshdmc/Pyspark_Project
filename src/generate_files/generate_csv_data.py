import csv
import os
import random
from datetime import datetime, timedelta

customer_ids = list(range(1, 21))
store_ids = list(range(121, 124))
product_data = {
    "Apple iPhone 14 Pro": 134999, "Sony WH-1000XM5 Headphones": 29990, "Samsung 55-inch 4K QLED TV": 84990,
    "Dell XPS 13 Laptop": 124999, "Apple Watch Series 9": 45900, "Kindle Paperwhite": 14999,
    "GoPro Hero 12": 54990, "IKEA Leather Recliner Chair": 35999, "Wakefit Orthopedic Mattress": 18999,
    "Home Centre Wooden Dining Table": 42999, "Philips Air Purifier": 18999, "Bosch Dishwasher": 58990,
    "Prestige Induction Cooktop": 4990, "KitchenAid Stand Mixer": 35990, "Dyson V12 Vacuum Cleaner": 65900
}
sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9]
}

start_date = datetime(2023, 3, 3)
end_date = datetime(2023, 8, 20)

file_location = "C:\\Users\\nikita\\Documents\\data_engineering\\spark_data"
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