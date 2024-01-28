import psycopg2 as pg
import config as conf
import pandas as pd

def connect_to_postgresql():
    try:
        # Create a connection to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=conf.dbname,
            user=conf.user,
            password=conf.password,
            host=conf.host,
            port=conf.port
        )
        return conn

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None

def create_table_from_dataframe(df, conn, table_name):
    cursor = conn.cursor()

    # Get the column names and data types from the DataFrame
    columns = df.columns
    dtypes = df.dtypes

    # Generate the CREATE TABLE statement
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for column, dtype in zip(columns, dtypes):
        data_type = dtype.name.lower()  # Get the lowercase name of the data type
        create_table_sql += f"{column} {data_type}, "
    create_table_sql = create_table_sql.rstrip(", ") + ");"

    try:
        # Execute the CREATE TABLE statement
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")

    except (Exception, psycopg2.Error) as error:
        print("Error while creating table:", error)

    finally:
        cursor.close()

def ingest_dataframe_into_table(df, conn, table_name):
    cursor = conn.cursor()

    try:
        # Convert the DataFrame to a list of tuples for insertion
        records = [tuple(row) for row in df.to_records(index=False)]

        # Generate the INSERT INTO statement
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s' for _ in df.columns])
        insert_into_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"

        # Execute the INSERT INTO statement
        cursor.executemany(insert_into_sql, records)
        conn.commit()
        print(f"Data ingested into table '{table_name}' successfully.")

    except (Exception, psycopg2.Error) as error:
        print("Error while ingesting data into table:", error)

    finally:
        cursor.close()

