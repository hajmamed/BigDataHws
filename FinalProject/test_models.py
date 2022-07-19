import datetime

import random

import pandas as pd
import seaborn as sb
import folium
import matplotlib.pyplot as plt
# from pyspark.ml.clustering import KMeans

from sklearn.cluster import KMeans

from yellowbrick.cluster import KElbowVisualizer
from sklearn.utils import shuffle
import numpy as np
import matplotlib.cm as cm
import matplotlib.colors as colors
from joblib import dump, load


# ******************************* Read Data
df = pd.read_csv("data.csv")
# ******************************* Shuffle Data
df = shuffle(df)
# ******************************* Split Data into train and test
train_count = int(0.2 * len(df))
train_data = df[:train_count]
# ******************************* prepare data for clustering
cols = ['Lat', 'Lon',]
values_for_clustering = train_data[cols]
print(values_for_clustering.dtypes)
# ******************************* Find best clusters count
best_clusters_count = 4
# ******************************* Do Clustering by KMeans
kmeans = KMeans(n_clusters=best_clusters_count, random_state=0)
kmeans.fit(values_for_clustering)

dump(kmeans, 'testk.joblib')
clf = load('testk.joblib')

print("after query")

