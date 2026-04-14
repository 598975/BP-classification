from pathlib import Path
import sys

import pandas as pd


sys.path.append(str(Path(__file__).parents[1]))
from util.filtering.lang_identification import identify_language_yaml
from db.database import Database
from util.filtering.structural_diff import filter_similar_blueprints


def filter_blueprints(db: Database):
    bps = {bp.id: bp for bp in db.get_all_blueprints()}
    bp_df = pd.DataFrame(
        [
            {_attr: getattr(bp, _attr) for _attr in bp.__dict__.keys()}
            for bp in bps.values()
        ]
    )
    bp_df["language"] = bp_df["blueprint_code"].apply(identify_language_yaml)
    bp_df_en = bp_df[bp_df["language"] == "en"]

    filtered_bp_df = filter_similar_blueprints(bp_df_en, threshold=0.1)

    # Drop columns that may not exist or are not needed
    columns_to_drop = [
        "language",
        "_sa_instance_state",
        "post",
        "topic_title",
        "post_content",
    ]
    existing_columns_to_drop = [
        col for col in columns_to_drop if col in filtered_bp_df.columns
    ]
    filtered_bp_df = filtered_bp_df.drop(columns=existing_columns_to_drop)

    filtered_bp_df = filtered_bp_df.reset_index(drop=True)

    db.update_blueprint_filtered_table(
        filtered_bp_df,
    )
