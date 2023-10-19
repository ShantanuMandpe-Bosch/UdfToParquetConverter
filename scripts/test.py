# -*- coding: utf-8 -*-

import pandas as pd
song_embeddings = pd.read_parquet('../main.parquet')
song_embeddings.to_csv('csvExample.csv')
print(song_embeddings)