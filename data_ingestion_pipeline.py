import pandas as pd
import os 
import config as conf
import pgadmin as pg
import sys

def read_csv_files_in_folder(folder_path):
    df = pd.DataFrame()

    

    return combined_df


def main(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        if filename.endswith(".csv"):
            print(filename)
            
            df = pd.read_csv(file_path)
            conn = pg.connect_to_postgresql()
            sys.exit()
            pg.create_table_from_dataframe(df, conn, filename)
            print('Table '+filename+' hase been created')
            pg.ingest_dataframe_into_table(df, conn, filename)
            print('Data has been ingested to '+filename)
            
folder_path = conf.folder_path

main(folder_path)