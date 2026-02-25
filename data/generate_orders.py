"""
Generates sample orders CSV files for the pipeline examples.

Usage:
    python data/generate_orders.py
"""

import csv
import os

ORDERS = [
    # orders for 2025-01-15
    {"order_id": "ORD001", "customer_name": "Alice",   "product": "Laptop",       "quantity": 1, "price": 999.99, "order_date": "2025-01-15"},
    {"order_id": "ORD002", "customer_name": "Bob",     "product": "Mouse",        "quantity": 2, "price": 25.50,  "order_date": "2025-01-15"},
    {"order_id": "ORD003", "customer_name": "Charlie", "product": "Keyboard",     "quantity": 1, "price": 75.00,  "order_date": "2025-01-15"},
    {"order_id": "ORD004", "customer_name": "Diana",   "product": "Monitor",      "quantity": 1, "price": 349.99, "order_date": "2025-01-15"},
    {"order_id": "ORD005", "customer_name": "Eve",     "product": "USB Cable",    "quantity": 3, "price": 12.99,  "order_date": "2025-01-15"},
    {"order_id": "ORD006", "customer_name": "Frank",   "product": "Webcam",       "quantity": 1, "price": 89.99,  "order_date": "2025-01-15"},
    {"order_id": "ORD007", "customer_name": "Grace",   "product": "Headphones",   "quantity": 1, "price": 199.99, "order_date": "2025-01-15"},
    {"order_id": "ORD008", "customer_name": "Hank",    "product": "Mouse Pad",    "quantity": 2, "price": 15.00,  "order_date": "2025-01-15"},
    {"order_id": "ORD009", "customer_name": "Ivy",     "product": "Desk Lamp",    "quantity": 1, "price": 45.00,  "order_date": "2025-01-15"},
    {"order_id": "ORD010", "customer_name": "Jack",    "product": "Notebook",     "quantity": 5, "price": 8.99,   "order_date": "2025-01-15"},
    # orders for 2025-01-16
    {"order_id": "ORD011", "customer_name": "Karen",   "product": "Tablet",       "quantity": 1, "price": 499.99, "order_date": "2025-01-16"},
    {"order_id": "ORD012", "customer_name": "Leo",     "product": "Phone Case",   "quantity": 2, "price": 19.99,  "order_date": "2025-01-16"},
    {"order_id": "ORD013", "customer_name": "Mona",    "product": "Charger",      "quantity": 1, "price": 29.99,  "order_date": "2025-01-16"},
    {"order_id": "ORD014", "customer_name": "Nick",    "product": "SSD 1TB",      "quantity": 1, "price": 109.99, "order_date": "2025-01-16"},
    {"order_id": "ORD015", "customer_name": "Olivia",  "product": "RAM 16GB",     "quantity": 2, "price": 64.99,  "order_date": "2025-01-16"},
    {"order_id": "ORD016", "customer_name": "Paul",    "product": "HDMI Cable",   "quantity": 3, "price": 14.99,  "order_date": "2025-01-16"},
    {"order_id": "ORD017", "customer_name": "Quinn",   "product": "Webcam",       "quantity": 1, "price": 89.99,  "order_date": "2025-01-16"},
    {"order_id": "ORD018", "customer_name": "Rachel",  "product": "Keyboard",     "quantity": 1, "price": 75.00,  "order_date": "2025-01-16"},
    {"order_id": "ORD019", "customer_name": "Sam",     "product": "Monitor",      "quantity": 1, "price": 349.99, "order_date": "2025-01-16"},
    {"order_id": "ORD020", "customer_name": "Tina",    "product": "Laptop Stand", "quantity": 1, "price": 59.99,  "order_date": "2025-01-16"},
]

# Re-delivery of the 2025-01-15 batch, but with UPDATED quantities for ORD001 and ORD002.
# This simulates a correction arriving from the source system.
ORDERS_REDELIVERY = [
    {"order_id": "ORD001", "customer_name": "Alice",   "product": "Laptop",     "quantity": 2, "price": 999.99, "order_date": "2025-01-15"},
    {"order_id": "ORD002", "customer_name": "Bob",     "product": "Mouse",      "quantity": 5, "price": 25.50,  "order_date": "2025-01-15"},
    {"order_id": "ORD003", "customer_name": "Charlie", "product": "Keyboard",   "quantity": 1, "price": 75.00,  "order_date": "2025-01-15"},
    {"order_id": "ORD004", "customer_name": "Diana",   "product": "Monitor",    "quantity": 1, "price": 349.99, "order_date": "2025-01-15"},
    {"order_id": "ORD005", "customer_name": "Eve",     "product": "USB Cable",  "quantity": 3, "price": 12.99,  "order_date": "2025-01-15"},
    {"order_id": "ORD006", "customer_name": "Frank",   "product": "Webcam",     "quantity": 1, "price": 89.99,  "order_date": "2025-01-15"},
    {"order_id": "ORD007", "customer_name": "Grace",   "product": "Headphones", "quantity": 1, "price": 199.99, "order_date": "2025-01-15"},
    {"order_id": "ORD008", "customer_name": "Hank",    "product": "Mouse Pad",  "quantity": 2, "price": 15.00,  "order_date": "2025-01-15"},
    {"order_id": "ORD009", "customer_name": "Ivy",     "product": "Desk Lamp",  "quantity": 1, "price": 45.00,  "order_date": "2025-01-15"},
    {"order_id": "ORD010", "customer_name": "Jack",    "product": "Notebook",   "quantity": 5, "price": 8.99,   "order_date": "2025-01-15"},
]


def main():
    output_dir = os.path.join(os.path.dirname(__file__), "input")
    os.makedirs(output_dir, exist_ok=True)

    fieldnames = ["order_id", "customer_name", "product", "quantity", "price", "order_date"]

    orders_path = os.path.join(output_dir, "orders.csv")
    with open(orders_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ORDERS)
    print(f"Created {orders_path} ({len(ORDERS)} rows)")

    redelivery_path = os.path.join(output_dir, "orders_redelivery.csv")
    with open(redelivery_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ORDERS_REDELIVERY)
    print(f"Created {redelivery_path} ({len(ORDERS_REDELIVERY)} rows)")


if __name__ == "__main__":
    main()
