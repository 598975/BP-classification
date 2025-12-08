from db.models import Blueprint
from util.text_manipulation import normalize_text
from util.text_manipulation import parse_yaml
from db.database import Database
from deepdiff import DeepDiff


def normalize_blueprint(obj):
    if isinstance(obj, dict):
        return {k: normalize_blueprint(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, list):
        return [normalize_blueprint(v) for v in obj]
    else:
        return normalize_text(str(obj))


def load_and_normalize_blueprints(topic_id=None, bps=None):
    if bps:
        return [normalize_blueprint(parse_yaml(bp.blueprint_code)) for bp in bps]
    db = Database()

    topic_posts = db.get_posts_by_topic_id(topic_id)
    topic_bps = [db.get_blueprints_by_post_id(post.post_id) for post in topic_posts]
    topic_bps = [bp for sublist in topic_bps for bp in sublist]
    normalized_codes = [
        normalize_blueprint(parse_yaml(bp.blueprint_code)) for bp in topic_bps
    ]
    return normalized_codes


def structural_diff(code1, code2):
    diff = DeepDiff(code1, code2, ignore_order=True)
    diff_size = len(str(diff))
    total_size = len(str(code1)) + len(str(code2))
    return diff, 1 - diff_size / total_size


def compare_multiple_bps(bps: list[Blueprint]) -> list[tuple[Blueprint, Blueprint, float]]:
    """
    Compare multiple blueprints and return their pairwise structural differences.
    
    :param bps: List of Blueprint objects to compare.
    :type bps: list[Blueprint]
    :return: List of tuples containing pairs of Blueprints and their similarity score.
    :rtype: list[tuple[Blueprint, Blueprint, float]]
    """
    
    normalized_codes = load_and_normalize_blueprints(bps=bps)
    comparison = []
    for i in range(len(normalized_codes)):
        for j in range(i + 1, len(normalized_codes)):
            _, similarity = structural_diff(normalized_codes[i], normalized_codes[j])
            comparison.append((bps[i], bps[j], similarity))
    return comparison

