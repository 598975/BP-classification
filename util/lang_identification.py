import langid
from .text_manipulation import parse_yaml, get_leaf_values
from db.models import Blueprint

""" def main() -> tuple[list[dict], Counter]:
    blueprints = fetch_blueprint_code()
    language_counts = Counter()
    non_english_bps : list[dict] = []
    
    with open("lang_output.txt", "w", encoding="utf-8") as f:
        for bp in tqdm.tqdm(blueprints, desc="Processing Blueprints"):
            bp_yaml = parse_yaml(bp[1])
            name = bp_yaml.get("blueprint", {}).get("name", "Unnamed Blueprint")
            bp_text = " ".join(str(value) for value in get_leaf_values(bp_yaml))
            language = langid.classify(bp_text)[0]
            language_counts[language] += 1
            if not language == "en":
                non_english_bps.append({"id": bp[0], "lang": language})
                f.write(f"Blueprint Name: {name}, Non-key text: {bp_text}\nDetected Language: {language}\n")
                f.write("-" * 100 + "\n")
            
    return non_english_bps, language_counts """

def identify_language(BP: Blueprint) -> str:
    bp_code = BP.blueprint_code
    bl_code_parsed = parse_yaml(bp_code)
    bp_text = " ".join(str(value) for value in get_leaf_values(bl_code_parsed))
    language = langid.classify(bp_text)[0]
    return language
    