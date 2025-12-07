import langid
from .text_manipulation import parse_yaml, get_leaf_values


def identify_language_yaml(bp_code) -> str:
    bl_code_parsed = parse_yaml(bp_code)
    bp_text = " ".join(str(value) for value in get_leaf_values(bl_code_parsed))
    language = langid.classify(bp_text)[0]
    return language

