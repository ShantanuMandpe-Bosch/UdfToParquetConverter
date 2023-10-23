filePath = 'main.parquet'
output = 'main.csv'

import pandas as pd
df = pd.read_parquet(filePath)
df.to_csv(output)
print(df)