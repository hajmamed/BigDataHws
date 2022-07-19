import random

import pandas as pd
import seaborn as sb
import folium
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from yellowbrick.cluster import KElbowVisualizer
from sklearn.utils import shuffle
import numpy as np
import matplotlib.cm as cm
import matplotlib.colors as colors
import pickle

# ******************************* Read Data
df = pd.read_csv("data.csv")
# ******************************* Shuffle Data
df = shuffle(df)
# ******************************* Split Data into train and test
train_count = int(0.2 * len(df))
train_data = df[:train_count]
# ******************************* prepare data for clustering
oneHotBases = pd.get_dummies(train_data.Base, prefix='Base')
train_data = train_data.join(oneHotBases)
print(train_data.head())
cols = ['Lat', 'Lon', ]
for col in oneHotBases.columns.values:
    cols.append(col)

values_for_clustering = train_data[cols]
print(values_for_clustering.dtypes)
# ******************************* Find best clusters count
model = KMeans()
visualizer = KElbowVisualizer(model, k=(1, 10))
visualizer.fit(values_for_clustering)
visualizer.show(outpath="out/base_kelbow_minibatchkmeans.png")
visualizer.show()

best_clusters_count = visualizer.elbow_value_
# best_clusters_count = 4
# ******************************* Do Clustering by KMeans
kmeans = KMeans(n_clusters=best_clusters_count, random_state=0)
kmeans.fit(values_for_clustering)

# ******************************* Save the model to disk
model_filename = 'out/base_model.mdl'
file = open(model_filename, 'wb')
pickle.dump(kmeans, file)
file.close()
print("Model Save successfully")
# ******************************* Get Clusters centroids
centroids_k = kmeans.cluster_centers_
print(centroids_k)
# ******************************* Plot centroids
# clocation_k = pd.DataFrame(centroids_k, columns=['Latitude', 'Longitude'])
clocation_k = pd.DataFrame(centroids_k, columns=cols)
print(clocation_k)

plt.scatter(clocation_k['Lat'], clocation_k['Lon'], marker="x", s=200)
centroid_k = clocation_k.values.tolist()
plt.savefig("out/base_centroids.png")
plt.show()
# ******************************* mark centroids on map
map_k = folium.Map(location=[clocation_k['Lat'].mean(), clocation_k['Lon'].mean()], zoom_start=10)
for point in centroid_k:
    folium.Marker([point[0], point[1]], popup=[point[0], point[1]]).add_to(map_k)
map_k.save("out/base_map.html ")
# ******************************* Save cluster labels to df
label_k = kmeans.labels_
# print(label_k)
df_new_k = train_data.copy()
df_new_k['Clusters'] = label_k
# print(df_new_k)
# ******************************* Plot clusters population
sb.catplot(data=df_new_k, x="Clusters", kind="count", height=7, aspect=2)
plt.savefig("out/base_factorplot.png")
plt.show()
# ******************************* Plot points colored by clustered
plt.scatter(df_new_k['Lat'], df_new_k['Lon'], c=label_k, cmap='viridis');
plt.savefig("out/base_scatter.png")
plt.show()

# ******************************* Mark All points on map
# map_clusters = folium.Map(location=[df_new_k['Lat'].mean(), df_new_k['Lon'].mean()], zoom_start=5)
map_clusters = folium.Map(location=[clocation_k['Lat'].mean(), clocation_k['Lon'].mean()], zoom_start=5)
# map_clusters = folium.Map(location=[40.71600413400166, -73.98971408426613], zoom_start=5)

# set color scheme for the clusters
x = np.arange(best_clusters_count)
ys = [i + x + (i * x) ** 2 for i in range(best_clusters_count)]
colors_array = cm.rainbow(np.linspace(0, 1, len(ys)))
rainbow = [colors.rgb2hex(i) for i in colors_array]

# add markers to the map
# markers_colors = []
# counter = 1
# for index, point in df_new_k.iterrows():
#     print("Add points to map " + str(counter) + "/" + str(len(df_new_k)), end="\r")
#     counter += 1
#     folium.vector_layers.CircleMarker(
#         [point['Lat'], point['Lon']],
#         radius=5,
#         # popup=label,
#         tooltip='Cluster ' + str(point['Clusters']),
#         color=rainbow[int(point['Clusters']) - 1],
#         fill=True,
#         fill_color=rainbow[int(point['Clusters']) - 1],
#         fill_opacity=0.9).add_to(map_clusters)
# map_clusters.save("out/base_map2.html ")

print("Finished")
