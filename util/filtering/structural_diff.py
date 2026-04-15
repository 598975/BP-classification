import sys
from pathlib import Path

import pandas as pd
from deepdiff import DeepDiff
from tqdm import tqdm

sys.path.append(str(Path(__file__).parents[1]))
from util.text_manipulation import normalize_text, parse_yaml


def normalize_blueprint(obj):
    if isinstance(obj, dict):
        return {k: normalize_blueprint(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, list):
        return [normalize_blueprint(v) for v in obj]
    else:
        return normalize_text(str(obj))


def structural_diff(code1, code2):
    diff = DeepDiff(
        code1,
        code2,
        ignore_order=True,
        get_deep_distance=True,
        cutoff_distance_for_pairs=1,
        cutoff_intersection_for_pairs=1,
    )
    return diff, float(diff["deep_distance"]) if "deep_distance" in diff else 0.0


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
        for _, code2, similarity in comparisons:
            if similarity <= threshold:
                similar_codes.add(code2)
        bp_df = bp_df.drop(
            bp_df[
                (bp_df["topic_id"] == topic_id)
                & (bp_df["blueprint_code"].isin(similar_codes))
            ].index
        )
    return bp_df
