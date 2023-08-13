"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv


def write_csv(csv_file, table_name):
    with psycopg2.connect(
        host="localhost",
        database="north",
        user="postgres",
        password="2so1lTrf6JY-"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name}")
            rows = cur.fetchall()

            with open(f"north_data/{csv_file}", 'r') as file:
                reader = csv.reader(file)
                next(reader)

                for row in reader:
                    insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(row))})"
                    cur.execute(insert_query, row)
                    conn.commit()

    conn.close()


write_csv('customers_data.csv', 'customers')
write_csv('employees_data.csv', 'employees')
write_csv('orders_data.csv', 'orders')
