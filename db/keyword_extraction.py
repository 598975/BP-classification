import ast
import re
import pandas as pd
from collections import Counter
import sys
from pathlib import Path
from tqdm import tqdm
import yake
from sklearn.feature_extraction.text import TfidfVectorizer

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
    
def update_blueprint_topic_keywords_yake(db: Database):
    topics = db.get_populated_topics()
    for topic in tqdm(topics, desc="Updating topic keywords for blueprints"):
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
            db.update_blueprint_topic_keywords(bp.id, topic_keywords, session)
        session.commit()
        session.close()
    
def update_blueprint_topic_keywords_tfidf(db: Database):
    blueprints = db.get_all_blueprints()
    topics = db.get_topics()
    posts = db.get_posts()
    bp_data = []
    for bp in blueprints:
        bp_dict = {
            "blueprint_id": bp.id,
            "blueprint_code": bp.blueprint_code,
            "post_id": bp.post_id,
        }
        bp_data.append(bp_dict)
    df_bp = pd.DataFrame(bp_data)
    topics_data = []
    for topic in topics:
        topic_dict = {
            "id": topic.id,
            "topic_id": topic.topic_id,
            "title": topic.title,
            "tags": topic.tags,
        }
        topics_data.append(topic_dict)
    df_topics = pd.DataFrame(topics_data)
    posts_data = []
    for post in posts:
        post_dict = {
            "id": post.id,
            "post_id": post.post_id,
            "topic_id": post.topic_id,
            "cooked": post.cooked,
        }
        posts_data.append(post_dict)
    df_posts = pd.DataFrame(posts_data)
    topic_posts = df_topics.merge(df_posts, left_on="topic_id", right_on="topic_id")
    topic_bp = topic_posts.merge(df_bp, left_on="post_id", right_on="post_id")

    corpus = []
    for topic in tqdm(topic_posts["topic_id"].unique(), desc="Preprocessing and grouping by topic"):
        topic_subset = topic_posts[topic_posts["topic_id"] == topic]
        texts = topic_subset["cooked"].tolist()
        combined_text = " ".join([tfidf_preprocessing(text) for text in texts])
        corpus.append(combined_text)


if __name__ == "__main__":
    db = Database()
    update_blueprint_keywords(db)
    update_blueprint_topic_keywords_yake(db)
