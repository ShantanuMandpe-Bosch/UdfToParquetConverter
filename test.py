# -*- coding: utf-8 -*-

import pandas as pd
song_embeddings = pd.read_parquet('main.parquet')
print(song_embeddings)