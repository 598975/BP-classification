import os
import random

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

random.seed(42)
np.random.seed(42)
os.environ["PYTHONHASHSEED"] = "42"


def cluster_kmeans(bp_df, X_normalized):
    kmeans = KMeans(n_clusters=55, random_state=42, n_init="auto")
    bp_df["fine_cluster"] = kmeans.fit_predict(X_normalized)

    centroids = kmeans.cluster_centers_
    sort_keys = [tuple(c) for c in centroids]
    sorted_indices = sorted(range(len(sort_keys)), key=lambda i: sort_keys[i])
    centroids_sorted = centroids[sorted_indices]
    cluster_mapping = {
        old_idx: new_idx for new_idx, old_idx in enumerate(sorted_indices)
    }
    bp_df["fine_cluster"] = bp_df["fine_cluster"].map(cluster_mapping)
    return bp_df, centroids_sorted
