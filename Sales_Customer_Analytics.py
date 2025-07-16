import sqlite3
import pandas as pd 
from faker import Faker 
import random 
import os
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
from datetime import datetime, timedelta


fake=Faker('en_US')
def generate_customers(n=100):
  customer=[]
  for i in range(1,n+1):
    customer.append((
      i,
      fake.first_name(),
      fake.last_name(),
      fake.email(),
      fake.city(),
      fake.country(),
      fake.date_of_birth(minimum_age=20,maximum_age=70)
    ))
  return customer
  

categories=['Electronics', 'Clothing', 'Books', 'Home & Kitchen', 'Sports']
def generate_products(n=60):
  products=[]
  for i in range(1,n+1):
    category=random.choice(categories)
    products.append((
      i,
      fake.word().capitalize(),
      category,
      round(random.uniform(10,500),2)
    ))
  return products


def generate_orders(n=500, customer_count=100, product_count=60):
  orders=[]
  for i in range(1, n+1):
    orders.append((
      i,
      random.randint(1, customer_count),
      random.randint(1, product_count),
      random.randint(1,10),
      fake.date_between(start_date='-2y', end_date='today')
    ))
  return orders 


def create_and_populate():
  if os.path.exists('ecommerce.db'):
    os.remove('ecommerce.db')
    print("🗑️ The old file ecommerce.db has been deleted")

  conn=sqlite3.connect('ecommerce.db')
  cur=conn.cursor()

  # Creation of tables and data entry
  cur.execute('''
  CREATE TABLE IF NOT EXISTS customers(
              id INTEGER PRIMARY KEY,
              first_name TEXT,
              last_name TEXT,
              email TEXT,
              city TEXT,
              country TEXT,
              birthdate DATE           
  )
  ''')
    
  cur.execute('''
  CREATE TABLE IF NOT EXISTS products (
              id INTEGER PRIMARY KEY,
              name TEXT,
              category TEXT,
              price REAL                        
  )
  ''')

  cur.execute('''
  CREATE TABLE IF NOT EXISTS orders(
              id INTEGER PRIMARY KEY,
              customer_id INTEGER,
              product_id INTEGER,
              quantity INTEGER,
              order_date DATE,
              FOREIGN KEY (customer_id) REFERENCES customers(id),
              FOREIGN KEY (product_id) REFERENCES products(id)
  )
  ''')

  cur.executemany('INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?)',generate_customers())
  cur.executemany('INSERT INTO products VALUES (?, ?, ?, ?)',generate_products())
  cur.executemany('INSERT INTO orders VALUES (?, ?, ?, ?, ?)',generate_orders())
  conn.commit()
  conn.close()
  print("✅ Η βάση δεδομένων ecommerce.db δημιουργήθηκε με επιτυχία!")


def transfer_sqlite_to_mysql(sqlite_path='ecommerce.db'):
  sqlite_conn=sqlite3.connect(sqlite_path)

  # Connection with MySQL
  mysql_conn=mysql.connector.connect(
    host="localhost",
    user="root",
    password="Manos1231!",
    database="ecommerce"
    )
    
  mysql_cursor=mysql_conn.cursor()

  # Load all the Data from SQLite
  df_customers=pd.read_sql_query("SELECT * FROM customers",sqlite_conn)
  df_products=pd.read_sql_query("SELECT * FROM products",sqlite_conn)
  df_orders=pd.read_sql_query("SELECT * FROM orders",sqlite_conn)

  for _, row in df_customers.iterrows():
    mysql_cursor.execute('''
      INSERT INTO customers(id, first_name, last_name, email, city, country, birthdate)
                        VALUES(%s,%s,%s,%s,%s,%s,%s)
    ''',tuple(row))

  for _, row in df_products.iterrows():
    mysql_cursor.execute('''
      INSERT INTO products(id, name, category, price)
                         VALUES(%s,%s,%s,%s)
    ''',tuple(row))

  for _, row in df_orders.iterrows():
    mysql_cursor.execute('''
      INSERT INTO orders(id, customer_id, product_id, quantity, order_date)
                          VALUES(%s,%s,%s,%s,%s)
    ''',tuple(row))

  mysql_conn.commit()
  mysql_conn.close()
  sqlite_conn.close()

  print("✅ The data was successfully transferred from SQLite to MySQL.")


def explore_data(db_path='ecommerce.db'):
  conn=sqlite3.connect(db_path)
  df_customers=pd.read_sql_query("SELECT * FROM customers",conn)
  df_products=pd.read_sql_query("SELECT * FROM products",conn)
  df_orders=pd.read_sql_query("SELECT * FROM orders",conn)
  conn.close()

  print("📊 Client:")
  print(df_customers.head(),"\n")
  print("🛍️ Products:")
  print(df_products.head(),"\n")
  print("📦 Orders")
  print(df_orders.head(),"\n")

  print("🔢 Customer amount:", df_customers.shape[0])
  print("🔢 Products amount:", df_products.shape[0])
  print("🔢 Orders amount:", df_orders.shape[0],"\n")

  print("📊 Statistic for Orders:")
  print(df_orders.describe(), "\n")

  # Null Data 
  print("❓ Check for NULL Data")
  print("Customers:\n", df_customers.isnull().sum())
  print("Products:\n", df_products.isnull().sum())
  print("Orders:\n",df_orders.isnull().sum())

  df_merged= df_orders.merge(df_customers, left_on='customer_id', right_on='id', suffixes=('','_cust'))
  df_merged= df_merged.merge(df_products, left_on='product_id', right_on='id', suffixes=('','_prod'))
  print(df_merged.head())
  return df_customers,df_products,df_orders 


def calculate_kpis(db_path='ecommerce.db'):
  conn=sqlite3.connect(db_path)
  df_customers=pd.read_sql_query("SELECT * FROM customers",conn)
  df_products=pd.read_sql_query("SELECT * FROM products",conn)
  df_orders=pd.read_sql_query("SELECT * FROM orders",conn)
  conn.close()

   # 🧮 Merged Data
  merged=df_orders.merge(df_customers, left_on="customer_id", right_on="id")\
          .merge(df_products, left_on="product_id", right_on="id", suffixes=('_customer', '_product'))
  print(merged)

  merged["total_price"]=merged["quantity"] * merged["price"]

  print("📊 🔝 TOP 5 Customers Based On Orders:")
  top_customers=merged.groupby(["first_name","last_name","city"]).size().sort_values(ascending=False).head(5)
  print(top_customers, "\n")

  print("📦 💰 Top 5 Products by Revenue:")
  product_revenue=merged.groupby("name")["total_price"].sum().sort_values(ascending=False).head(5)
  print(product_revenue)

  print("Monthly Expenses:")
  merged["order_date"]=pd.to_datetime(merged["order_date"])
  merged["order_month"]=merged["order_date"].dt.to_period("M")
  monthly_sales=merged.groupby("order_month")["total_price"].sum()

  print("🏆 Most Profitable Product:")
  top_product=product_revenue.idxmax()
  print(f" ➡️ {top_product} ")
  return merged


def export_to_csv(db_path="ecommerce.db"):
  conn=sqlite3.connect(db_path)
  df_customers=pd.read_sql_query("SELECT * FROM customers",conn)
  df_products=pd.read_sql_query("SELECT * FROM products",conn)
  df_orders=pd.read_sql_query("SELECT * FROM orders",conn)
  conn.close()
  df_customers.to_csv("customers.csv",index=False)
  df_products.to_csv("products.csv",index=False)
  df_orders.to_csv("orders.csv",index=False)
  print("✅The data was saved to CSV files (customers.csv, products.csv, orders.csv)")
  return df_customers, df_products, df_orders


def plot_data( df_customers, df_products, df_orders):
  df_full=df_orders.merge(df_products, left_on='product_id', right_on='id') \
                    .merge(df_customers, left_on='customer_id', right_on='id',suffixes=('_product', '_customer'))
  df_full['total_price']=df_full['quantity']*df_full['price']

  # 📦 Top 10 Products by Revenue
  plt.figure(figsize=(12,6))
  top_order=df_full.groupby('name')['total_price'].sum().sort_values(ascending=False).head(10)
  sns.barplot(x=top_order.values, y=top_order.index, palette='Blues_r')
  plt.title("🔝 Top 10 Products by Highest Revenue")
  plt.xlabel("Total Revenue")
  plt.ylabel("Product Name")
  plt.tight_layout()
  plt.show()

  # 🌍 Orders by Country
  plt.figure(figsize=(10,6))
  orders_per_country=df_full.groupby('country').size().sort_values(ascending=False)
  sns.barplot(x=orders_per_country.values, y=orders_per_country.index, palette='Greens_r')
  plt.title("🌍 Number of Orders Per Country")
  plt.xlabel("Number Of Orders")
  plt.ylabel("Country")
  plt.tight_layout()
  plt.show()

  # 👥 Top 10 Customers by Total Revenue
  plt.figure(figsize=(12,6))
  revenue_per_customer=df_full.groupby(['first_name','last_name'])['total_price'].sum().sort_values(ascending=False).head(10)
  revenue_per_customer.index=[f"{fname} {lname}" for fname, lname in revenue_per_customer.index]
  sns.barplot(x=revenue_per_customer.values, y=revenue_per_customer.index, palette='Purples_r')
  plt.title("👥 Top 10 Costumers by Revevue")
  plt.xlabel("Total Revenue ($)")
  plt.ylabel("Costomer Name")
  plt.tight_layout()
  plt.show()

  # 📆 Sells by Months
  plt.figure(figsize=(12,6))
  df_full['order_date']=pd.to_datetime(df_full['order_date'])
  df_full['order_month']=df_full['order_date'].dt.to_period("M").astype(str)
  monthly_sales=df_full.groupby('order_month')['total_price'].sum()
  sns.lineplot(x=monthly_sales.index, y=monthly_sales.values, markers='o', color='orange')
  plt.title("📈 Monthly Revenue Over Time")
  plt.xlabel("Month")
  plt.ylabel("Revenue ($)")
  plt.xticks(rotation=45)
  plt.tight_layout()
  plt.show()

  
def main():
  create_and_populate()
  transfer_sqlite_to_mysql() 
  explore_data()
  calculate_kpis()
  [cst,prd,ord]=export_to_csv()
  plot_data(cst,prd,ord)


if __name__=='__main__':
  main()