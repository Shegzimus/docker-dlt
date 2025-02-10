import dlt.sources.rest_api
import requests
import json
import pandas as pd
from datetime import datetime
from datetime import date
import dlt
from dlt.sources.helpers import requests


key = ''

URL = f"https://v6.exchangerate-api.com/v6/{key}/latest/EUR"

response = requests.get(URL)

# if response.status_code == 200:
#     data = response.json()
#     rates = data["conversion_rates"]
#     print(json.dumps(rates, indent=2))
# else:
#     print(f"Error: {response.status_code}")

data = response.json()

#
df = pd.json_normalize(data['conversion_rates'])
df = df.melt().reset_index()
df["index"] += 1
df["date"] = date.today()
df = df.rename(columns={'index': 'id',
                        'variable':'currencyCode', 
                        'value': 'fxrate'
                        })

records = df.to_dict(orient='records')

pipeline = dlt.pipeline(
    pipeline_name='fxrate_pipeline',
    destination='postgres',
    dataset_name='incremental'
)

load_info = pipeline.run(
    records,
    write_disposition="merge",
    primary_key= ("currencyCode", "date"),
    table_name= "fxrates",
    dataset_name='fxrate_db'
)

print(load_info)