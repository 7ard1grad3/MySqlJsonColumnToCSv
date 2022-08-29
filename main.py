import json
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

try:
    load_dotenv()
    # Sql connection string
    mydb = create_engine(f"mysql+pymysql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASS')}@"
                         f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
    df = pd.read_sql("Select tracking_number,checkpoints from courier_transaction_checkpoints;", mydb)
    # field names
    fields = ['tracking_number', 'tag', 'zip', 'city', 'slug',
              'state', 'message', 'location', 'country_name',
              'checkpoint_time']
    csv_rows = []
    for _, row in df.iterrows():
        if row['checkpoints']:
            # make sure that column data is iterable
            checkpoints = iter(json.loads(row['checkpoints']))
            for checkpoint in checkpoints:
                csv_row = [row['tracking_number']]
                # Fill only listed columns
                for field in fields[1:]:
                    try:
                        value = checkpoint.get(field, '')
                    except Exception as e:
                        value = ""
                    csv_row.append(value)
                csv_rows.append(csv_row)
    # Save dataframe to csv
    pd.DataFrame(csv_rows, columns=fields).to_csv('list.csv', index=False, encoding='utf-8')
except Exception as e:
    print(str(e))
