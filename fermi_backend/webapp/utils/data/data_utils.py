import csv, re
import pandas as pd
from io import StringIO

def decode_bytes_obj(obj: bytes, file_type: str, path):
    if file_type == 'csv':
        data = str(obj, 'utf-8')
        data = StringIO(data)
        df = pd.read_csv(data)
        if path:
            df.to_csv(path, index=False)
        return df



