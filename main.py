import json
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


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
kpp = pd.DataFrame(csv_rows, columns=fields)
# Convert to datetime
kpp.checkpoint_time = pd.to_datetime(kpp.checkpoint_time, format='%Y-%m-%d %H:%M:%S', utc=True)
# Sort by checkpoint_time
kpp.sort_values(by='checkpoint_time', inplace=True)
# Group by tracking numbers
for df_name, df_group in kpp.groupby('tracking_number'):
    # Go over indexes in the group
    # enumerate creates new indexes for the list 1 -> len()
    for idx, df_index in enumerate(df_group.index):
        if 0 < idx <= len(df_group.index)-1:
            # Get the next index from the group indexes and calculate the GAP
            cur_row = kpp.at[df_group.index[idx], 'checkpoint_time']
            prev_row = kpp.at[df_group.index[idx-1], 'checkpoint_time']
            kpp.loc[df_group.index[idx], 'time_gap'] = (cur_row - prev_row).total_seconds() / 60.0
        else:
            kpp.loc[df_group.index[idx], 'time_gap'] = 0
kpp.to_csv('list.csv', index=False, encoding='utf-8')