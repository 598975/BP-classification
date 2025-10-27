import pandas as pd
from collections import Counter
import sys
from pathlib import Path
from tqdm import tqdm

# Add the parent directory to the path to import modules
sys.path.append(str(Path(__file__).parents[1]))
from db.database import Database
from utils.blueprint import expand_blueprint, extract_keywords
from utils.text_manipulation import parse_yaml, normalize_text


def count_keywords(keywords_section):
    return dict(Counter([normalize_text(x) for x in keywords_section]))


# Process each row in df_bp and accumulate results
def process_row(row):
    bp_dict = parse_yaml(row["blueprint_code"])
    bp_dict_expanded = expand_blueprint(bp_dict)
    keywords = extract_keywords(bp_dict_expanded)
    # trigger and condition are projected to input becasue they are inputs to the system
    project = {
        "trigger": "input",
        "condition": "input",
        "action": "output",
    }
    all_keyword_count = {}
    for section in ["trigger", "condition", "action"]:
        section_count = count_keywords(keywords[section])
        for k, v in section_count.items():
            key = f"{project[section]}__{k}"
            if key in all_keyword_count:
                all_keyword_count[key] += v
            else:
                all_keyword_count[key] = v
    return all_keyword_count


def update_blueprint_keywords(db: Database):
    blueprints = db.get_all_blueprints()

    data = []
    for bp in blueprints:
        bp_dict = {
            "blueprint_id": bp.id,
            "blueprint_code": bp.blueprint_code,
        }
        data.append(bp_dict)
    df_bp = pd.DataFrame(data)

    # Apply the process to each row and create a new DataFrame from the results
    keyword_count_dicts = []
    for _, row in tqdm(
        df_bp.iterrows(), total=len(df_bp), desc="Extracting keywords from blueprints"
    ):
        keyword_count_dicts.append(process_row(row))

    # Update keyword counts in the database
    session = db.open_session()
    for bp_id, keywords in tqdm(
        zip(df_bp["blueprint_id"], keyword_count_dicts),
        total=len(df_bp),
        desc="Updating keyword counts in the database",
    ):
        db.update_blueprint_keywords(bp_id, keywords, session)
    session.commit()
    session.close()


if __name__ == "__main__":
    db = Database()
    update_blueprint_keywords(db)
