import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime
import random
import os
import yaml # pyright: ignore[reportMissingModuleSource]

fake = Faker('en_IN')
random.seed(42)
np.random.seed(42)

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def messy_date(date_obj):
    """Returns date in either clean or messy format — Windows compatible"""
    fmt = random.choice(['clean', 'messy'])
    if fmt == 'clean':
        return date_obj.strftime('%Y-%m-%d')
    else:
        # Windows-safe: manually remove leading zeros
        return f"{date_obj.month}/{date_obj.day}/{date_obj.year}"

def generate_customers(n):
    print(f"Generating {n} customers...")
    customers = []
    for i in range(1, n + 1):
        login_date = fake.date_between(start_date='-1y', end_date='today')
        customers.append({
            'CustomerID': f'CUST_{i:04d}',
            'FirstName': fake.first_name(),
            'LastName': fake.last_name(),
            'Email': fake.email(),
            'PhoneNumber': fake.phone_number(),
            'City': random.choice(['Bangalore', 'Mumbai', 'Delhi', 'Chennai',
                                   'Hyderabad', 'Pune', 'Kolkata', 'Ahmedabad']),
            'SignupDate': fake.date_between(start_date='-3y', end_date='today'),
            'LastLoginDate': random.choice([messy_date(login_date), None])
        })
    return pd.DataFrame(customers)

def generate_products(n):
    print(f"Generating {n} products...")
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Kitchen',
                  'Sports', 'Beauty', 'Toys', 'Grocery']
    products = []
    for i in range(1, n + 1):
        category = random.choice(categories)
        price = round(random.uniform(99, 9999), 2)
        products.append({
            'ProductID': f'PROD_{i:04d}',
            'ProductName': fake.bs().title(),
            'Category': category,
            'Price': random.choice([price, f"₹{price}"]),
            'StockQuantity': random.randint(0, 500),
            'Supplier': fake.company(),
        })
    return pd.DataFrame(products)

def generate_orders(n, customer_ids, product_ids, start_date, end_date):
    print(f"Generating {n} orders...")
    statuses = ['delivered', 'DELIVERED', 'Delivered',
                'shipped', 'SHIPPED',
                'cancelled', 'Cancelled',
                'pending', 'PENDING']
    orders = []
    for i in range(1, n + 1):
        order_date = fake.date_between(
            start_date=datetime.strptime(start_date, '%Y-%m-%d'),
            end_date=datetime.strptime(end_date, '%Y-%m-%d')
        )
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(99, 9999), 2)
        orders.append({
            'OrderID': f'ORD_{i:06d}',
            'CustomerID': random.choice(customer_ids),
            'ProductID': random.choice(product_ids),
            'OrderDate': messy_date(order_date),
            'Quantity': quantity,
            'UnitPrice': unit_price,
            'TotalAmount': round(quantity * unit_price, 2),
            'Status': random.choice(statuses),
            'PaymentMethod': random.choice(['UPI', 'Credit Card', 'Debit Card',
                                            'Net Banking', 'COD', None]),
            'ShippingCity': random.choice([fake.city(), fake.city(), None])
        })
    return pd.DataFrame(orders)

def main():
    config = load_config()
    cfg = config['data']

    customers_df = generate_customers(cfg['num_customers'])
    products_df = generate_products(cfg['num_products'])
    orders_df = generate_orders(
        cfg['num_orders'],
        customers_df['CustomerID'].tolist(),
        products_df['ProductID'].tolist(),
        cfg['start_date'],
        cfg['end_date']
    )

    output_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'sample')
    os.makedirs(output_dir, exist_ok=True)

    customers_df.to_csv(f'{output_dir}/customers.csv', index=False)
    products_df.to_csv(f'{output_dir}/products.csv', index=False)
    orders_df.to_csv(f'{output_dir}/orders.csv', index=False)

    print(f"\n✅ Data generated successfully:")
    print(f"   customers.csv  — {len(customers_df):,} rows")
    print(f"   products.csv   — {len(products_df):,} rows")
    print(f"   orders.csv     — {len(orders_df):,} rows")
    print(f"\nSaved to: {output_dir}")

if __name__ == '__main__':
    main()