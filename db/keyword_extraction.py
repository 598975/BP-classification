import json
import pandas as pd
from collections import Counter
import sys
from pathlib import Path
from tqdm import tqdm
import yake
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

# Add the parent directory to the path to import modules
sys.path.append(str(Path(__file__).parents[1]))
from db.database import Database
from util.blueprint import expand_blueprint, extract_keywords
from util.text_manipulation import (
    parse_yaml,
    normalize_text,
    preprocess,
    tfidf_preprocessing,
    keywords_remove_input,
)
from util.pandas import get_dataframes


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


def update_blueprint_keywords_yake(db: Database):
    bp_df, posts_df, topics_df = get_dataframes(db)
    bp_df["processed_keywords"] = bp_df["extracted_keywords"].apply(
        keywords_remove_input
    )

    session = db.open_session()
    try:
        for _, topic in tqdm(
            topics_df.iterrows(),
            total=topics_df.shape[0],
            desc="Extracting YAKE keywords",
        ):
            posts_in_topic = posts_df[posts_df["topic_id"] == topic["topic_id"]]
            bps_in_topic = bp_df[bp_df["topic_id"] == topic["topic_id"]]

            yake_kw = yake.KeywordExtractor(n=2)
            tags_set = set(topic["tags"])
            proc_keywords = bps_in_topic["processed_keywords"].tolist()
            proc_keywords = [
                kwd for sublist in proc_keywords if sublist for kwd in sublist
            ]
            yake_kw.stopword_set = yake_kw.stopword_set.union(
                {"blueprint", "home", "assistant", "automation"}
                # | tags_set
                # | set(proc_keywords)
            )

            text = [preprocess(topic["title"])]
            for post in posts_in_topic["cooked"].tolist():
                text.append(preprocess(post))
            for bp in bps_in_topic["description"].tolist():
                text.append(preprocess(bp))

            text = ". ".join(text)
            _kws = yake_kw.extract_keywords(text)
            keywords = [kwd for kwd, _ in _kws]

            for _, bp in bps_in_topic.iterrows():
                bp_df.loc[bp_df["id"] == bp["id"], "keywords_yake"] = json.dumps(
                    keywords[0:4]
                )
                db.update_yake_keywords(bp["id"], keywords[0:4], session)
    finally:
        session.commit()
        session.close()
    bp_df["processed_keywords"] = bp_df["processed_keywords"].apply(json.dumps)
    db.update_blueprint_filtered_table(bp_df)


def extract_top_n_keywords(row, features, top_n=2):
    row_array = row.toarray().flatten()
    top_n_indices = np.argsort(row_array)[-top_n:][::-1]
    top_n_terms = [features[i] for i in top_n_indices]
    top_n_scores = [row_array[i] for i in top_n_indices]
    return list(zip(top_n_terms, top_n_scores))


def update_blueprint_keywords_tfidf(db: Database):
    bp_df, posts_df, topics_df = get_dataframes(db)

    corpus = []
    topic_to_index = {}
    for idx, (_, topic) in enumerate(
        tqdm(
            topics_df.iterrows(),
            total=topics_df.shape[0],
            desc="Building TF-IDF corpus",
        )
    ):
        posts_in_topic = posts_df[posts_df["topic_id"] == topic["topic_id"]]
        texts = posts_in_topic["cooked"].tolist()
        texts.insert(0, topic["title"])

        bps_in_topic = bp_df[bp_df["topic_id"] == topic["topic_id"]]
        texts.extend(bps_in_topic["description"].tolist())
        texts.extend(bps_in_topic["name"].tolist())

        combined_text = " ".join(
            [tfidf_preprocessing(text, topic["tags"]) for text in texts]
        )
        corpus.append(combined_text)
        topic_to_index[topic["topic_id"]] = idx

    tfidf = TfidfVectorizer(min_df=1, max_df=0.95)
    tfidf_matrix = tfidf.fit_transform(corpus)
    feature_names = tfidf.get_feature_names_out()

    session = db.open_session()
    for _, topic in tqdm(
        topics_df.iterrows(), total=topics_df.shape[0], desc="Updating TF-IDF keywords"
    ):
        topic_id = topic["topic_id"]
        topic_index = topic_to_index[topic_id]
        row = tfidf_matrix[topic_index]
        top_keywords = extract_top_n_keywords(row, feature_names, top_n=2)
        topic_keywords = {kw: score for kw, score in top_keywords}

        bps_in_topic = bp_df[bp_df["topic_id"] == topic_id]
        for _, bp in bps_in_topic.iterrows():
            db.update_tfidf_keywords(bp["id"], topic_keywords, session)
    session.commit()
    session.close()


if __name__ == "__main__":
    db = Database()
    # update_blueprint_keywords(db)
    update_blueprint_keywords_yake(db)
    # update_blueprint_keywords_tfidf(db)
