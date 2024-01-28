import pandas as pd
import os 
import config as conf
import pgadmin as pg
import sys

def pipeline(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        if filename.endswith(".csv"):
            table_name = filename.split(".")[0]
            
            df = pd.read_csv(file_path)
            df = df.rename(columns={'desc': 'description'})
            # Drop unnamed columns
            df = df.loc[:, df.columns != '']
            print(df.columns)
            df.to_csv(file_path,index=False)
            # sys.exit()
            conn = pg.connect_to_postgresql()
            print("Connection Established")
            
            pg.create_table_from_dataframe(df, conn, table_name)
            
            # pg.ingest_dataframe_into_table(df, conn, table_name)

            pg.copy_csv_into_table(file_path, conn, table_name)


            # sys.exit()


if __name__ == '__main__':
    folder_path = conf.folder_path

    pipeline(folder_path)