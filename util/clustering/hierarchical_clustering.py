import os
import random

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering

random.seed(42)
np.random.seed(42)
os.environ["PYTHONHASHSEED"] = "42"


def cluster_hierarchical(bp_df, centroids_sorted):
    agg = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=0.4,
        metric="cosine",
        linkage="average",
        compute_distances=True,
    )

    model = agg.fit(centroids_sorted)
    top_labels = model.labels_
    bp_df["top_cluster"] = bp_df["fine_cluster"].map(dict(enumerate(top_labels)))
    return bp_df
