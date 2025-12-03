import ast
import re
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
from util.text_manipulation import parse_yaml, normalize_text, yake_preprocessing, tfidf_preprocessing


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
    topics = db.get_populated_topics()
    for topic in tqdm(topics, desc="Updating topic yake keywords"):
        posts = db.get_posts_by_topic_id(topic.topic_id)
        bps = db.get_blueprints_by_topic_id(topic.topic_id)
        topic_bps = []
        for post in posts:
            bps = db.get_blueprints_by_post_id(post.post_id)
            topic_bps.extend(bps)
        cleaned_texts = [yake_preprocessing(post.cooked) for post in posts]
        cleaned_texts.insert(0, yake_preprocessing(topic.title))
        combined_text = ' '.join(cleaned_texts)
        
        yake_kw_extractor = yake.KeywordExtractor(lan="en", n=1, top=3)
        yake_kw_extractor.stopword_set.add("blueprint")
        tags = topic.tags
        if isinstance(tags, str):
            try:
                tags = ast.literal_eval(tags)
            except (ValueError, SyntaxError):
                tags = [tags]
        for tag in tags:
            yake_kw_extractor.stopword_set.add(tag.lower())
        keywords = yake_kw_extractor.extract_keywords(combined_text)
        topic_keywords = {kw: score for kw, score in keywords}
        
        session = db.open_session()
        for bp in topic_bps:
            db.update_yake_keywords(bp.id, topic_keywords, session)
        session.commit()
        session.close()
        
def extract_top_n_keywords(row, features, top_n=2):
    row_array = row.toarray().flatten()
    top_n_indices = np.argsort(row_array)[-top_n:][::-1]
    top_n_terms = [features[i] for i in top_n_indices]
    top_n_scores = [row_array[i] for i in top_n_indices]
    return list(zip(top_n_terms, top_n_scores))
    
def update_blueprint_keywords_tfidf(db: Database):
    blueprints = db.get_all_blueprints()
    topics = db.get_topics()
    posts = db.get_posts()
    
    bp_data = [{
            "blueprint_id": bp.id,
            "blueprint_code": bp.blueprint_code,
            "description": bp.description,
            "name": bp.name,
            "post_id": bp.post_id,
        } for bp in blueprints]
    df_bp = pd.DataFrame(bp_data)
    topics_data = [{
            "id": topic.id,
            "topic_id": topic.topic_id,
            "title": topic.title,
            "tags": topic.tags,
        } for topic in topics]
    df_topics = pd.DataFrame(topics_data)
    posts_data = [{
            "id": post.id,
            "post_id": post.post_id,
            "topic_id": post.topic_id,
            "cooked": post.cooked,
        } for post in posts]
    df_posts = pd.DataFrame(posts_data)

    topic_posts = df_topics.merge(df_posts, on="topic_id")
    topic_bp = topic_posts.merge(df_bp, on="post_id")
    topic_posts = topic_posts[topic_posts["topic_id"].isin(topic_bp["topic_id"])]

    corpus = []
    topic_to_index = {}
    for idx, topic in tqdm(enumerate(topic_posts["topic_id"].unique()), desc="Preprocessing and creating corpus by topic"):
        topic_subset = topic_posts[topic_posts["topic_id"] == topic]
        texts = topic_subset["cooked"].tolist()
        texts.insert(0, topic_subset["title"].iloc[0])
        bps = topic_bp[topic_bp["topic_id"] == topic]
        texts.extend(bps["description"].tolist())
        texts.extend(bps["name"].tolist())
        combined_text = " ".join([tfidf_preprocessing(text, topic_subset["tags"].iloc[0]) for text in texts])
        corpus.append(combined_text)
        topic_to_index[topic] = idx
    
    tfidf = TfidfVectorizer()
    res = tfidf.fit_transform(corpus)
    feature_names = tfidf.get_feature_names_out()
    
    session = db.open_session()
    for topic_id in tqdm(topic_bp["topic_id"].unique(), desc="Updating topic tfidf keywords"):
        topic_index = topic_to_index[topic_id]
        row = res[topic_index]
        top_keywords = extract_top_n_keywords(row, feature_names, top_n=2)
        topic_keywords = {kw: score for kw, score in top_keywords}

        topic_bps = topic_bp[topic_bp["topic_id"] == topic_id]
        for _, bp_row in topic_bps.iterrows():
            db.update_tfidf_keywords(bp_row["blueprint_id"], topic_keywords, session)
    session.commit()
    session.close()


if __name__ == "__main__":
    db = Database()
    update_blueprint_keywords(db)
    update_blueprint_keywords_yake(db)
    update_blueprint_keywords_tfidf(db)