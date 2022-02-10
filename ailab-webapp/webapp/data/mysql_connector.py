import mysql.connector as msql
import pandas as pd
import os
from mysql.connector import Error
import numpy as np

if __name__ == '__main__':

    conn = msql.connect(host='localhost', user='root', database='cds',
                        password='Tiger980330!')
    if conn.is_connected():
        cursor = conn.cursor()
        path_to_dir = '/Users/xuanmingcui/Downloads/cds-$75K(GFI)'
        directory = os.fsencode(path_to_dir)
        for file in os.listdir(directory):
            filename = os.fsdecode(file)
            if filename.endswith(".csv"):
                df = pd.read_csv(os.path.join(path_to_dir, filename), skiprows=[0])
                df.dropna(subset=['QUOTE_ID'], inplace=True)
                df.drop_duplicates(subset=['QUOTE_ID'], inplace=True)
                df = df.replace({np.nan: None})
                print('importing file: ', filename)
                for i, row in df.iterrows():
                    str_format = '(' + ','.join((['%s'] * len(row))) + ')'
                    sql = "INSERT INTO cds.CDSData VALUES " + str_format
                    cursor.execute(sql, tuple(row))
                    conn.commit()
        print('Finished')
