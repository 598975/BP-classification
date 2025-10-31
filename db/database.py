import sys
from pathlib import Path
from logging import debug, info, error
from dotenv import load_dotenv
import os
from sqlalchemy import cast, Integer, text, func, select
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm
import sqlite3
import pandas as pd

# Add the parent directory to the path to import modules
sys.path.append(str(Path(__file__).parents[1]))
from db.models import Base, Topic, Post, Blueprint, BlueprintFTS, init_database

DATABASE_NAME = "home_assistant_blueprints.sqlite"
SCHEMA_FILE = "db/schema.sql"
TOPICS_TABLE = "topics"
POSTS_TABLE = "posts"
BLUEPRINTS_FTS_TABLE = "blueprints_fts"
POSTGRESQL_HOST_NAME = "imap.new.foldr.org"
POSTGRESQL_DB_NAME = "ha_crawler"

load_dotenv()


class Database:
    def __init__(
        self,
        database_name=DATABASE_NAME,
        schema_file=SCHEMA_FILE,
        topcis_table=TOPICS_TABLE,
        posts_table=POSTS_TABLE,
        blueprints_fts_table=BLUEPRINTS_FTS_TABLE,
        postgresql_host_name=POSTGRESQL_HOST_NAME,
        postgresql_db_name=POSTGRESQL_DB_NAME,
        local=True,
        drop_existing_tables=False,
    ):
        self.database_name = database_name
        self.schema_file = schema_file
        self.topics_table = topcis_table
        self.posts_table = posts_table
        self.blueprints_fts_table = blueprints_fts_table
        self.postqresql_host_name = postgresql_host_name
        self.postgresql_db_name = postgresql_db_name
        self.postgresql_username = os.getenv("POSTGRESQL_USERNAME")
        self.postgresql_password = os.getenv("POSTGRESQL_PASSWORD")
        self.local = local
        self.engine = self.init_db(blueprints_fts_table, drop_existing_tables)
        self.create_tables()

    def init_db(self, blueprints_fts_table, drop_existing_tables):
        try:
            database_url = f"postgresql://{self.postgresql_username}:{self.postgresql_password}@{self.postqresql_host_name}/{self.postgresql_db_name}"
            if self.local:
                database_url = f"sqlite:///{self.database_name}"
            engine = init_database(
                database_url, self.local, blueprints_fts_table, drop_existing_tables
            )
            info("Database setup successfully.")
            return engine
        except Exception as e:
            error(f"Error initializing the database: {e}")
            raise e

    def create_tables(self):
        try:
            Base.metadata.create_all(self.engine)
            info("Tables created successfully.")
        except Exception as e:
            error(f"Error creating tables: {e}")
            raise e

    def open_session(self):
        Session = sessionmaker(bind=self.engine)
        return Session()

    def _insert_topic(self, session, topic_id, **kwargs):
        topic = Topic(topic_id=topic_id, **kwargs)
        session.add(topic)

    def _update_topic(self, session, topic_id, **kwargs):
        topic = session.query(Topic).filter_by(topic_id=topic_id).first()
        for key, value in kwargs.items():
            setattr(topic, key, value)

    def check_topic_exists(self, session, topic_id):
        topic = session.query(Topic).filter_by(topic_id=topic_id).first()
        return bool(topic)

    def upsert_topic(self, session, topic_id, force_insert=False, **kwargs):
        debug(f"Upserting topic: {topic_id}")
        if force_insert or not self.check_topic_exists(session, topic_id):
            self._insert_topic(session, topic_id, **kwargs)
            debug(f"Topic inserted: {topic_id}")
        else:
            self._update_topic(session, topic_id, **kwargs)
            debug(f"Topic updated: {topic_id}")

    def get_topics_count(self):
        session = self.open_session()
        count = session.query(Topic).count()
        session.close()
        return count

    def _insert_post(self, session, post_id, **kwargs):
        post = Post(post_id=post_id, **kwargs)
        session.add(post)

    def _update_post(self, session, post_id, **kwargs):
        post = session.query(Post).filter_by(post_id=post_id).first()
        for key, value in kwargs.items():
            setattr(post, key, value)

    def _check_post_exists(self, session, post_id):
        post = session.query(Post).filter_by(post_id=post_id).first()
        return bool(post)

    def upsert_post(self, session, post_id, force_insert=False, **kwargs):
        debug(f"Upserting post: {post_id}")
        if force_insert or not self._check_post_exists(session, post_id):
            self._insert_post(session, post_id, **kwargs)
            debug(f"Post inserted: {post_id}")
        else:
            self._update_post(session, post_id, **kwargs)
            debug(f"Post updated: {post_id}")

    def _insert_blueprint(self, session, blueprint_url, **kwargs):
        blueprint = Blueprint(blueprint_url=blueprint_url, **kwargs)
        session.add(blueprint)
        blueprint_id = blueprint.id
        return blueprint_id

    def _update_blueprint(self, session, blueprint_url, **kwargs):
        blueprint = (
            session.query(Blueprint).filter_by(blueprint_url=blueprint_url).first()
        )
        blueprint_id = blueprint.id
        for key, value in kwargs.items():
            setattr(blueprint, key, value)
        return blueprint_id

    def _check_blueprint_url_exists(self, blueprint_url):
        session = self.open_session()
        blueprint = (
            session.query(Blueprint).filter_by(blueprint_url=blueprint_url).first()
        )
        session.close()
        return bool(blueprint)

    def check_blueprint_hash_exists(self, blueprint_hash, session):
        blueprint = (
            session.query(Blueprint).filter_by(blueprint_hash=blueprint_hash).first()
        )
        return bool(blueprint)

    def upsert_blueprint(self, session, blueprint_url, force_insert=False, **kwargs):
        debug(f"Upserting blueprint: {blueprint_url}")
        if force_insert or not self._check_blueprint_url_exists(blueprint_url):
            blueprint_id = self._insert_blueprint(session, blueprint_url, **kwargs)
            debug(f"Blueprint inserted: {blueprint_url}")
        else:
            blueprint_id = self._update_blueprint(session, blueprint_url, **kwargs)
            debug(f"Blueprint updated: {blueprint_url}")
        return blueprint_id

    def _insert_blueprint_fts(self, session, blueprint_id, **kwargs):
        if self.local:
            self._insert_blueprint_fts_sqlite(session, blueprint_id, **kwargs)
        else:
            self._insert_blueprint_fts_postgresql(session, blueprint_id, **kwargs)

    def _insert_blueprint_fts_postgresql(self, session, blueprint_id, **kwargs):
        blueprint_fts = BlueprintFTS(blueprint_id=blueprint_id, **kwargs)
        session.add(blueprint_fts)

    def _insert_blueprint_fts_sqlite(self, session, blueprint_id, **kwargs):
        insert_sql = f"""
            INSERT INTO {self.blueprints_fts_table} (blueprint_id, {", ".join(kwargs.keys())})
            VALUES (:blueprint_id, {", ".join(f":{key}" for key in kwargs.keys())})
        """
        params = {"blueprint_id": blueprint_id, **kwargs}
        session.execute(text(insert_sql), params=params)

    def _update_blueprint_fts(self, session, blueprint_id, **kwargs):
        if self.local:
            self._update_blueprint_fts_sqlite(session, blueprint_id, **kwargs)
        else:
            self._update_blueprint_fts_postgresql(session, blueprint_id, **kwargs)

    def _update_blueprint_fts_postgresql(self, session, blueprint_id, **kwargs):
        blueprint_fts = (
            session.query(BlueprintFTS).filter_by(blueprint_id=blueprint_id).first()
        )
        for key, value in kwargs.items():
            setattr(blueprint_fts, key, value)

    def _update_blueprint_fts_sqlite(self, session, blueprint_id, **kwargs):
        update_sql = f"""
            UPDATE {self.blueprints_fts_table}
            SET {", ".join(f"{key} = :{key}" for key in kwargs.keys())}
            WHERE blueprint_id = :blueprint_id
        """
        params = {"blueprint_id": blueprint_id, **kwargs}
        session.execute(text(update_sql), params=params)

    def _check_blueprint_fts_exists(self, session, blueprint_id):
        if self.local:
            return self._check_blueprint_fts_exists_sqlite(session, blueprint_id)
        else:
            return self._check_blueprint_fts_exists_postgresql(session, blueprint_id)

    def _check_blueprint_fts_exists_postgresql(self, session, blueprint_id):
        blueprint_fts = (
            session.query(BlueprintFTS).filter_by(blueprint_id=blueprint_id).first()
        )
        return bool(blueprint_fts)

    def _check_blueprint_fts_exists_sqlite(self, session, blueprint_id):
        select_sql = f"""
            SELECT blueprint_id
            FROM {self.blueprints_fts_table}
            WHERE blueprint_id = :blueprint_id
        """
        result = session.execute(
            text(select_sql), params={"blueprint_id": blueprint_id}
        )
        return bool(result.fetchone())

    def upsert_blueprint_fts(self, session, blueprint_id, force_insert=False, **kwargs):
        debug(f"Upserting blueprint on FTS table: {blueprint_id}")
        if force_insert or not self._check_blueprint_fts_exists(session, blueprint_id):
            self._insert_blueprint_fts(session, blueprint_id, **kwargs)
            debug(f"Blueprint inserted on FTS table: {blueprint_id}")
        else:
            self._update_blueprint_fts(session, blueprint_id, **kwargs)
            debug(f"Blueprint updated on FTS table: {blueprint_id}")

    def get_all_blueprints(self):
        session = self.open_session()
        blueprints = session.query(Blueprint).all()
        for blueprint in tqdm(blueprints, desc="Loading blueprints"):
            blueprint.topic_title = blueprint.post.topic.title
            blueprint.topic_id = blueprint.post.topic.topic_id
            blueprint.tags = blueprint.post.topic.tags
            blueprint.created_at = blueprint.post.created_at
            blueprint.post_content = blueprint.post.cooked

        session.close()
        return blueprints

    def get_blueprints_by_ids(self, blueprint_ids):
        session = self.open_session()
        blueprints = (
            session.query(Blueprint).filter(Blueprint.id.in_(blueprint_ids)).all()
        )
        for blueprint in tqdm(blueprints, desc="Loading blueprints"):
            blueprint.topic_title = blueprint.post.topic.title
            blueprint.created_at = blueprint.post.created_at
            blueprint.post_url = blueprint.post.post_url

        session.close()
        return blueprints

    def get_topics(self):
        session = self.open_session()
        topics = session.query(Topic).all()
        session.close()
        return topics

    def get_posts(self):
        session = self.open_session()
        posts = session.query(Post).all()
        session.close()
        return posts

    def update_blueprint_keywords(self, blueprint_id, keywords, session):
        blueprint = session.query(Blueprint).filter_by(id=blueprint_id).first()
        blueprint.extracted_keywords = keywords
        debug(f"Blueprint keywords updated: {blueprint_id}")

    def search_blueprint_by_keywords(
        self,
        input_keyword: str,
        input_operator: str,
        input_count: int,
        output_keyword: str,
        output_operator: str,
        output_count: int,
    ):
        # Create search conditions
        search_conditions = []
        if input_keyword != "":
            input_query_target = cast(
                text(f"extracted_keywords->>'input__{input_keyword}'"),
                Integer,
            )
            if input_operator == ">":
                input_query = input_query_target > input_count
            elif input_operator == "==":
                input_query = input_query_target == input_count
            elif input_operator == "<":
                input_query = input_query_target < input_count
            else:
                raise ValueError("Invalid operator")
            search_conditions.append(input_query)
        if output_keyword != "":
            output_query_target = cast(
                text(f"extracted_keywords->>'output__{output_keyword}'"),
                Integer,
            )
            if output_operator == ">":
                output_query = output_query_target > output_count
            elif output_operator == "==":
                output_query = output_query_target == output_count
            elif output_operator == "<":
                output_query = output_query_target < output_count
            else:
                raise ValueError("Invalid operator")
            search_conditions.append(output_query)

        # Run query
        session = self.open_session()
        query = (
            session.query(Blueprint)
            .filter(
                *search_conditions,
            )
            .limit(20)
        )
        result = query.all()
        for bp in result:
            bp.topic_title = bp.post.topic.title
            bp.topic_tags = bp.post.topic.tags
        session.close()
        return result

    def search_blueprint_by_fts_on_blueprint_code(self, query_string: str):
        if self.local:
            conn = sqlite3.connect("home_assistant_blueprints.sqlite")
            cursor = conn.cursor()
            cursor.execute(
                "SELECT blueprint_id, topic_title, blueprint_code, rank FROM blueprints_fts WHERE blueprint_expanded MATCH ? ORDER BY rank DESC LIMIT 20",
                (query_string,),
            )
            blueprints = cursor.fetchall()
            conn.close()
            result = pd.DataFrame(
                blueprints,
                columns=["blueprint_id", "topic_title", "blueprint_code", "rank"],
            )
            return result

        else:
            session = self.open_session()
            tsquery = func.plainto_tsquery("english", query_string)
            tsvector = func.to_tsvector("english", BlueprintFTS.blueprint_expanded)
            rank = func.ts_rank(tsvector, tsquery).label("rank")
            query = (
                session.query(BlueprintFTS, rank)
                .filter(tsvector.op("@@")(tsquery))
                .order_by(rank.desc())
            ).limit(20)
            blueprints = query.all()
            data = []
            for bp, rank in blueprints:
                if rank < 0.001:
                    continue
                bp_dict = {
                    "blueprint_id": bp.blueprint_id,
                    "topic_title": bp.topic_title,
                    "blueprint_code": bp.blueprint_code,
                    "rank": rank,
                }
                data.append(bp_dict)
            result = pd.DataFrame(data)
            session.close()
            return result

    def search_blueprint_by_fts_on_blueprint_sections(
        self, query_input: str, query_output: str
    ):
        if self.local:
            conn = sqlite3.connect("home_assistant_blueprints.sqlite")
            cursor = conn.cursor()

            query_parts = []
            query_params = []
            if query_input:
                query_parts.append("blueprint_input MATCH ?")
                query_params.append(query_input)
            if query_output:
                query_parts.append("blueprint_action MATCH ?")
                query_params.append(query_output)
            query_string = " OR ".join(query_parts)
            query_full = f"SELECT blueprint_id, topic_title, blueprint_code, rank FROM blueprints_fts WHERE {query_string} ORDER BY rank DESC LIMIT 20"

            cursor.execute(query_full, query_params)
            blueprints = cursor.fetchall()
            conn.close()
            result = pd.DataFrame(
                blueprints,
                columns=["blueprint_id", "topic_title", "blueprint_code", "rank"],
            )
            return result
        else:
            session = self.open_session()
            # tsquery
            tsquery_input = func.plainto_tsquery("english", query_input)
            tsquery_output = func.plainto_tsquery("english", query_output)
            # tsvector
            tsvector_input = func.to_tsvector("english", BlueprintFTS.blueprint_input)
            tsvector_output = func.to_tsvector("english", BlueprintFTS.blueprint_action)
            # rank
            rank_input = func.ts_rank(tsvector_input, tsquery_input)
            rank_output = func.ts_rank(tsvector_output, tsquery_output)
            combined_rank = (rank_input + rank_output).label("rank")
            # query
            operations = []
            if query_input != "":
                operations.append(tsvector_input.op("@@")(tsquery_input))
            if query_output != "":
                operations.append(tsvector_output.op("@@")(tsquery_output))
            query = (
                session.query(BlueprintFTS, combined_rank)
                .filter(
                    *operations,
                )
                .order_by(combined_rank.desc())
            ).limit(20)

            blueprints = query.all()
            
    def get_posts_by_topic_id(self, topic_id):
        session = self.open_session()
        stmt = (
            select(Post)
            .join(Topic)
            .where(Topic.id == topic_id)
        )
        posts = session.execute(stmt).scalars().all()
        session.close()
        return posts
    
    def get_blueprints_by_post_id(self, post_id):
        session = self.open_session()
        stmt = (
            select(Blueprint)
            .where(Blueprint.post_id == post_id)
        )
        blueprints = session.execute(stmt).scalars().all()
        session.close()
        return blueprints
    

