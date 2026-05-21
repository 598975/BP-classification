import os
import random

import numpy as np

from .feature_extraction import extract_features
from .hierarchical_clustering import cluster_hierarchical
from .kmeans_clustering import cluster_kmeans

from db.database import Database
from util.dataframe_utils import get_dataframes

random.seed(42)
np.random.seed(42)
os.environ["PYTHONHASHSEED"] = "42"


def cluster_blueprints(db: Database):
    bp_df, _, _ = get_dataframes(db)
    bp_df = bp_df.sort_values("id").reset_index(drop=True)

    bp_df, X_normalized = extract_features(bp_df)

    bp_df, centroids_sorted = cluster_kmeans(bp_df, X_normalized)

    bp_df = cluster_hierarchical(bp_df, centroids_sorted)

    if "embeddings" in bp_df.columns:
        bp_df = bp_df.drop(columns=["embeddings"])

    db.update_blueprint_categorized_table(bp_df)