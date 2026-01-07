import pandas as pd
from db.database import Database
from typing import Any


def get_dataframes(db: Database) -> tuple[Any, Any, Any]:
    bp_df = db.get_filtered_bps()
    posts = {post.post_id: post for post in db.get_posts()}
    topics = {topic.topic_id: topic for topic in db.get_topics()}

    posts_df = pd.DataFrame(
        [
            {_attr: getattr(post, _attr) for _attr in post.__dict__.keys()}
            for post in posts.values()
        ]
    )
    topics_df = pd.DataFrame(
        [
            {_attr: getattr(topic, _attr) for _attr in topic.__dict__.keys()}
            for topic in topics.values()
        ]
    )

    _filtered_topics = bp_df["topic_id"].unique().tolist()

    topics_df = topics_df[topics_df["topic_id"].isin(_filtered_topics)]
    posts_df = posts_df[posts_df["topic_id"].isin(_filtered_topics)]

    return bp_df, posts_df, topics_df
