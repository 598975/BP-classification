from db.models import Blueprint
from util.text_manipulation import normalize_text
from util.text_manipulation import parse_yaml
from db.database import Database
from deepdiff import DeepDiff
import tqdm
import pandas as pd


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
    diff = DeepDiff(code1, code2, ignore_order=True)
    diff_size = len(str(diff))
    total_size = len(str(code1)) + len(str(code2))
    return diff, 1 - diff_size / total_size


def compare_multiple_bps(codes):
    normalized_codes = [normalize_blueprint(parse_yaml(code)) for code in codes]
    comparison = []
    for i in range(len(normalized_codes)):
        for j in range(i + 1, len(normalized_codes)):
            _, similarity = structural_diff(normalized_codes[i], normalized_codes[j])
            comparison.append((codes[i], codes[j], similarity))
    return comparison


def filter_similar_blueprints(bp_df, threshold: float = 0.8) -> pd.DataFrame:
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
    for topic_id in tqdm.tqdm(unique_topics, desc="Filtering blueprints by topic"):
        topic_bp_df = bp_df[bp_df["topic_id"] == topic_id]
        codes = topic_bp_df["blueprint_code"].tolist()
        comparisons = compare_multiple_bps(codes)
        similar_codes = set()
        for code1, code2, similarity in comparisons:
            if similarity >= threshold:
                similar_codes.add(code2)
        bp_df = bp_df.drop(
            bp_df[(bp_df["topic_id"] == topic_id) & (bp_df["blueprint_code"].isin(similar_codes))].index
        )
    return bp_df


if __name__ == "__main__":
    from lang_identification import identify_language_yaml

    db = Database()
    bps = {bp.id: bp for bp in db.get_all_blueprints()}
    bp_df = pd.DataFrame(
        [
            {_attr: getattr(bp, _attr) for _attr in bp.__dict__.keys()}
            for bp in bps.values()
        ]
    )
    bp_df["language"] = bp_df["blueprint_code"].apply(identify_language_yaml)
    bp_df_en = bp_df[bp_df["language"] == "en"]

    filtered_bp_df = filter_similar_blueprints(bp_df_en, threshold=0.8)
    
    print(f"English blueprints: {len(bp_df_en)}")
    print(f"Unique blueprints: {len(filtered_bp_df)}")
