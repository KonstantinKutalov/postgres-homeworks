import psycopg2
import csv

# Подключение к базе данных
conn = psycopg2.connect(
    dbname="north",
    user="postgres",
    password="12345",
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

# Загрузка данных из файлов csv в таблицы

# Заполнение таблицы employees
with open('north_data/employees_data.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cursor.execute(
            "INSERT INTO employees (first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s)",
            (row['first_name'], row['last_name'], row['title'], row['birth_date'], row['notes'])
        )

# Заполнение таблицы customers
with open('north_data/customers_data.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cursor.execute(
            "INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)",
            (row['customer_id'], row['company_name'], row['contact_name'])
        )

# Заполнение таблицы orders
with open('north_data/orders_data.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cursor.execute(
            "INSERT INTO orders (customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, %s, %s)",
            (row['customer_id'], row['employee_id'], row['order_date'], row['ship_city'])
        )

conn.commit()
conn.close()
