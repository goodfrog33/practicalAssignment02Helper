import mysql.connector
import uuid
from faker import Faker
from dotenv import load_dotenv
import random
from datetime import datetime, timedelta
import os

# Load environment variables
load_dotenv()

HOST = os.getenv('host')       # Should match 'host' in .env
USER = os.getenv('user')       # Should match 'user' in .env
PASSWORD = os.getenv('password') # Should match 'password' in .env
DATABASE = os.getenv('database')


# Connect to the MySQL database
connection = mysql.connector.connect(
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DATABASE
)

cursor = connection.cursor()
fake = Faker()

# Create tables
print("Creating tables...")
cursor.execute('''
    CREATE TABLE IF NOT EXISTS opt_clients (
        id VARCHAR(36) PRIMARY KEY,
        name VARCHAR(255),
        surname VARCHAR(255),
        email VARCHAR(255),
        phone VARCHAR(50),
        address TEXT,
        status VARCHAR(10)
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS opt_products (
        product_id INT AUTO_INCREMENT PRIMARY KEY,
        product_name VARCHAR(255),
        product_category VARCHAR(255),
        description TEXT
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS opt_orders (
        order_id INT AUTO_INCREMENT PRIMARY KEY,
        order_date DATETIME,
        client_id VARCHAR(36),
        product_id INT,
        FOREIGN KEY (client_id) REFERENCES opt_clients(id),
        FOREIGN KEY (product_id) REFERENCES opt_products(product_id)
    );
''')

connection.commit()
print("Tables created.")

# Insert data into opt_clients
print("Inserting into opt_clients...")
client_insert_query = """
    INSERT INTO opt_clients (id, name, surname, email, phone, address, status) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
clients_data = [
    (str(uuid.uuid4()), fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(), fake.address(), random.choice(['active', 'inactive']))
    for _ in range(100000)
]
cursor.executemany(client_insert_query, clients_data)
connection.commit()
print("Inserted into opt_clients.")

# Insert data into opt_products
print("Inserting into opt_products...")
product_insert_query = """
    INSERT INTO opt_products (product_name, product_category, description) 
    VALUES (%s, %s, %s)
"""
categories = ['Category1', 'Category2', 'Category3', 'Category4', 'Category5']
products_data = [
    (fake.word(), random.choice(categories), fake.text())
    for _ in range(1000)
]
cursor.executemany(product_insert_query, products_data)
connection.commit()
print("Inserted into opt_products.")

# Insert data into opt_orders
print("Inserting into opt_orders...")
order_insert_query = """
    INSERT INTO opt_orders (order_date, client_id, product_id) 
    VALUES (%s, %s, %s)
"""
order_date_start = datetime.now() - timedelta(days=365 * 5)
orders_data = [
    (order_date_start + timedelta(days=random.randint(0, 365 * 5)), random.choice(clients_data)[0], random.randint(1, 1000))
    for _ in range(1000000)
]
# Use chunks to avoid memory issues
chunk_size = 10000
for i in range(0, len(orders_data), chunk_size):
    cursor.executemany(order_insert_query, orders_data[i:i + chunk_size])
    connection.commit()
    print(f"Inserted {i + chunk_size} rows into opt_orders...")

print("Inserted into opt_orders.")

# Execute non-optimized query
print("Executing non-optimized query...")
non_optimized_query = '''
    SELECT p.product_name, c.name, COUNT(o.order_id) AS order_count
    FROM opt_orders o
    JOIN opt_clients c ON o.client_id = c.id
    JOIN opt_products p ON o.product_id = p.product_id
    WHERE o.order_date > '2023-01-01'
    GROUP BY p.product_name, c.name
    ORDER BY order_count DESC;
'''
cursor.execute(non_optimized_query)
result = cursor.fetchall()
print(f"Non-optimized query result: {result[:5]}")  # Print first 5 rows for demonstration

# Execute optimized query using CTE and indexes
print("Creating indexes and executing optimized query...")
cursor.execute('''
    CREATE INDEX idx_opt_orders_order_date
    ON opt_orders(order_date);
''')

cursor.execute('''
    CREATE INDEX idx_opt_orders_client_id
    ON opt_orders(client_id);
''')

cursor.execute('''
    CREATE INDEX idx_opt_orders_product_id
    ON opt_orders(product_id);
''')

optimized_query = '''
    WITH recent_orders AS (
        SELECT order_id, client_id, product_id
        FROM opt_orders
        WHERE order_date > '2023-01-01'
    )
    SELECT p.product_name, c.name, COUNT(ro.order_id) AS order_count
    FROM recent_orders ro
    JOIN opt_clients c ON ro.client_id = c.id
    JOIN opt_products p ON ro.product_id = p.product_id
    GROUP BY p.product_name, c.name
    ORDER BY order_count DESC;
'''
cursor.execute(optimized_query)
result = cursor.fetchall()
print(f"Optimized query result: {result[:5]}")  # Print first 5 rows for demonstration

# Close the cursor and connection
cursor.close()
connection.close()