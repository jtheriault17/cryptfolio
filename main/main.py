import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime

load_dotenv()

def connect():
    connection = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    return connection

def init(connection):
    try:
        with connection.cursor() as cur:
            with open("db/schema.sql", "r") as f:
                sql = f.read()
                cur.execute(sql)
        connection.commit()
        print("Schema initialized successfully.")
    except Exception as e:
        connection.rollback()
        print(f"An error occurred: {e}")
    finally:
        connection.close()

def main():

    connection = connect()
    init(connection)


if __name__ == "__main__":
    main()
