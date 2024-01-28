import psycopg2 
import config as conf
import pandas as pd
from psycopg2 import sql

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
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    cursor.execute(drop_table_sql)
    conn.commit()
    print(f"Table '{table_name}' dropped.")
    # Get the column names and data types from the DataFrame
    columns = df.columns
    dtypes = df.dtypes
    i = 0
    for type in dtypes:
        if type == 'object':
            dtypes[i] = 'text'
        if type == 'float64':
            dtypes[i] = 'float'
        if type == 'int64':
            dtypes[i] = 'numeric'
           
        i += 1
    print(dtypes)
    # Generate the CREATE TABLE statement
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for column, dtype in zip(columns, dtypes):
        data_type = dtype.lower()
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
        for record in records:
            # Convert numpy.int64 to int for each record
            record = tuple(int(value) if isinstance(value, pd._libs.int_.Int64) else value for value in record)
            cursor.executemany(insert_into_sql, record)
        conn.commit()
        print(f"Data ingested into table '{table_name}' successfully.")

    except (Exception, psycopg2.Error) as error:
        print("Error while ingesting data into table:", error)

    finally:
        cursor.close()

def copy_csv_into_table(file_path, conn, table_name):
    cursor = conn.cursor()

    try:

        # Generate the SQL COPY statement
        copy_sql = sql.SQL("""
            COPY %s
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """%(table_name))

        # Open and read the CSV file
        with open(file_path, 'r') as file:
            cursor.copy_expert(sql=copy_sql, file=file)
            conn.commit()

        print(f"CSV data copied to PostgreSQL table '{table_name}' successfully.")

    except (Exception, psycopg2.Error) as error:
        print("Error copying data to PostgreSQL:", error)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()