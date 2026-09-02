import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_DIR = Path("/home/iceberg/data/raw")
OUTPUT_FILE = OUTPUT_DIR / "sales.csv"

PRODUCTS = [
    ("Laptop", "Electronics", 850.00),
    ("Mouse", "Electronics", 25.00),
    ("Keyboard", "Electronics", 45.00),
    ("Monitor", "Electronics", 220.00),
    ("Desk Chair", "Furniture", 180.00),
    ("Desk", "Furniture", 300.00),
    ("Notebook", "Stationery", 5.00),
    ("Pen", "Stationery", 2.00),
    ("Backpack", "Accessories", 55.00),
    ("Headphones", "Electronics", 90.00),
]

CITIES = [
    "Kathmandu",
    "Pokhara",
    "Lalitpur",
    "Bhaktapur",
    "Biratnagar",
    "Butwal",
]

PAYMENT_METHODS = [
    "cash",
    "card",
    "mobile_payment",
]

def generate_sales(row_count : int = 1000):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    start_date = datetime(2026,9,1)

    with OUTPUT_FILE.open("w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow(
            [
                "transaction_id",
                "transaction_timestamp",
                "product",
                "category",
                "quantity",
                "unit_price",
                "customer_city",
                "payment_method",
            ]           
        )

        for transaction_id in range(1, row_count + 1):
            product, category, unit_price = random.choice(PRODUCTS)

            quantity = random.randint(1, 5)

            timestamp = start_date + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )

            writer.writerow(
                [
                    transaction_id,
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    product,
                    category,
                    quantity,
                    unit_price,
                    random.choice(CITIES),
                    random.choice(PAYMENT_METHODS),
                ]
            )        


    print(f"Generated {row_count} sales records")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_sales()