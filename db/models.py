from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Boolean,
    JSON,
    ForeignKey,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Topic(Base):
    __tablename__ = "topics"

    id = Column(Integer, primary_key=True)
    first_post_cooked = Column(Text)
    topic_url = Column(Text)
    # The following columns are derived from the Discourse API `getSpecificPostsFromTopic`.
    # https://docs.discourse.org/#tag/Topics/operation/getSpecificPostsFromTopic
    topic_id = Column(String, unique=True)
    title = Column(String)
    fancy_title = Column(String)
    slug = Column(String)
    posts_count = Column(Integer)
    reply_count = Column(Integer)
    highest_post_number = Column(Integer)
    image_url = Column(Text)
    created_at = Column(DateTime)
    last_posted_at = Column(String)
    visible = Column(Boolean)
    closed = Column(Boolean)
    archived = Column(Boolean)
    archetype = Column(String)
    category_id = Column(Integer)
    pinned_globally = Column(Boolean)
    featured_link = Column(Text)
    word_count = Column(Integer)
    user_id = Column(Integer)
    pinned_at = Column(DateTime)
    pinned_until = Column(String)
    slow_mode_seconds = Column(Integer)
    draft = Column(Text)
    draft_key = Column(String)
    draft_sequence = Column(Integer)
    unpinned = Column(String)
    pinned = Column(Boolean)
    current_post_number = Column(Integer)
    chunk_size = Column(Integer)
    bookmarked = Column(Boolean)
    topic_timer = Column(String)
    message_bus_last_id = Column(Integer)
    participant_count = Column(Integer)
    show_read_indicator = Column(Boolean)
    thumbnails = Column(Text)
    slow_mode_enabled_until = Column(String)
    summarizable = Column(Boolean)
    post_stream = Column(Text)
    tags = Column(Text)
    tags_descriptions = Column(Text)
    crawled_at = Column(DateTime)
    views = Column(Integer)
    like_count = Column(Integer)

    # Relationship to Posts
    posts = relationship("Post", back_populates="topic")


class Post(Base):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True)
    post_url = Column(Text)
    # The following columns are derived from response.post_stream.posts of the Discourse API `getSpecificPostsFromTopic`.
    # https://docs.discourse.org/#tag/Topics/operation/getSpecificPostsFromTopic
    post_id = Column(String, unique=True)
    name = Column(String)
    username = Column(String)
    created_at = Column(DateTime)
    cooked = Column(Text)
    post_number = Column(Integer)
    post_type = Column(Integer)
    updated_at = Column(DateTime)
    reply_count = Column(Integer)
    reply_to_post_number = Column(Integer)
    quote_count = Column(Integer)
    incoming_link_count = Column(Integer)
    reads = Column(Integer)
    readers_count = Column(Integer)
    score = Column(Integer)
    topic_id = Column(String, ForeignKey("topics.topic_id"))

    # Relationship to Topic
    topic = relationship("Topic", back_populates="posts")
    blueprint = relationship("Blueprint", back_populates="post")


class Blueprint(Base):
    __tablename__ = "blueprints"

    id = Column(Integer, primary_key=True)
    blueprint_url = Column(Text)
    blueprint_code = Column(Text)
    blueprint_hash = Column(String, unique=True)
    post_id = Column(String, ForeignKey("posts.post_id"))
    name = Column(String)
    description = Column(Text)
    extracted_keywords = Column(JSON)
    topic_keywords = Column(JSON)
    keywords_yake = Column(JSON)
    keywords_tfidf = Column(JSON)

    # Relationship to Post
    post = relationship("Post", back_populates="blueprint")


class BlueprintFTS(Base):
    __tablename__ = "blueprints_fts"

    blueprint_id = Column(Integer, primary_key=True)
    blueprint_code = Column(Text)
    topic_title = Column(Text)
    blueprint_expanded = Column(Text)
    blueprint_declaraion = Column(Text)
    blueprint_trigger = Column(Text)
    blueprint_condition = Column(Text)
    blueprint_action = Column(Text)
    blueprint_input = Column(Text)
    post_content = Column(Text)


def init_database(
    database_url, local=False, BLUEPRINTS_FTS_TABLE=None, drop_existing_tables=False
):
    # Create the engine
    engine = create_engine(database_url, echo=False)

    if drop_existing_tables:
        # Drop the tables
        Base.metadata.drop_all(engine)
        # Manually drop the full text search table
        if local:
            with engine.connect() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {BLUEPRINTS_FTS_TABLE}"))

    # Create the tables
    tables_to_create = [Topic, Post, Blueprint]

    if local:
        # Create the full text search table
        fts_table_creation_sql = """
    CREATE VIRTUAL TABLE IF NOT EXISTS {blueprints_fts} USING FTS5(
        blueprint_id, blueprint_code, topic_title, blueprint_expanded, blueprint_declaraion, blueprint_trigger, blueprint_condition, blueprint_action, blueprint_input, post_content
    );
    """
        with engine.connect() as connection:
            connection.execute(
                text(fts_table_creation_sql.format(blueprints_fts=BLUEPRINTS_FTS_TABLE))
            )
    else:
        tables_to_create.append(BlueprintFTS)

    Base.metadata.create_all(engine)

    return engine
