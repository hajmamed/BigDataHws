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
from joblib import dump, load

