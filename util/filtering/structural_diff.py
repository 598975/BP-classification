import json
from deepdiff import DeepDiff
from tqdm import tqdm
import pandas as pd

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parents[1]))
from util.filtering.lang_identification import identify_language_yaml
from db.database import Database
from util.text_manipulation import normalize_text, parse_yaml


def normalize_blueprint(obj):
    if isinstance(obj, dict):
        return {k: normalize_blueprint(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, list):
        return [normalize_blueprint(v) for v in obj]
    else:
        return normalize_text(str(obj))


""" def load_and_normalize_blueprints(topic_id=None, bps=None):
    if bps:
        return [normalize_blueprint(parse_yaml(bp.blueprint_code)) for bp in bps]
    db = Database()

    topic_posts = db.get_posts_by_topic_id(topic_id)
    topic_bps = [db.get_blueprints_by_post_id(post.post_id) for post in topic_posts]
    topic_bps = [bp for sublist in topic_bps for bp in sublist]
    normalized_codes = [
        normalize_blueprint(parse_yaml(bp.blueprint_code)) for bp in topic_bps
    ]
    return normalized_codes """


def structural_diff(code1, code2):
    diff = DeepDiff(code1, code2, ignore_order=True, get_deep_distance=True)
    return diff, diff["deep_distance"] if "deep_distance" in diff else 0.0


def compare_multiple_bps(codes):
    normalized_codes = [normalize_blueprint(parse_yaml(code)) for code in codes]
    comparison = []
    for i in range(len(normalized_codes)):
        for j in range(i + 1, len(normalized_codes)):
            _, similarity = structural_diff(normalized_codes[i], normalized_codes[j])
            comparison.append((codes[i], codes[j], similarity))
    return comparison

def filter_similar_blueprints(
    bp_df: pd.DataFrame, threshold: float = 0.8
) -> pd.DataFrame:
    """
    Filter out similar blueprints based on structural similarity.

    :param bps: List of Blueprint objects to filter.
    :type bps: list[Blueprint]
    :param threshold: Similarity threshold above which blueprints are considered similar.
    :type threshold: float
    :return: List of unique Blueprint objects.
    :rtype: list[Blueprint]
    """

    unique_topics = bp_df["topic_id"].unique()
    for topic_id in tqdm(unique_topics, desc="Filtering blueprints by topic"):
        topic_bp_df = bp_df[bp_df["topic_id"] == topic_id]
        codes = topic_bp_df["blueprint_code"].tolist()
        comparisons = compare_multiple_bps(codes)
        similar_codes = set()
        for code1, code2, similarity in comparisons:
            if similarity <= threshold:
                similar_codes.add(code2)
        bp_df = bp_df.drop(
            bp_df[
                (bp_df["topic_id"] == topic_id)
                & (bp_df["blueprint_code"].isin(similar_codes))
            ].index
        )
    return bp_df




