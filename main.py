from db.database import Database
from db.keyword_extraction import update_blueprint_keywords, update_blueprint_keywords_tfidf, update_blueprint_keywords_yake
import logging
import argparse
from sqlalchemy.sql import text
from sqlalchemy import inspect

parser = argparse.ArgumentParser(
    description="Fetch and store topics from Home Assistant Community Forum."
)
""" parser.add_argument(
    "--fetch-new",
    action="store_true",
    help="Fetch and store the new topics in the blueprint-exchange category.",
)
parser.add_argument(
    "--fetch-all",
    action="store_true",
    help="Fetch and store all topics in the blueprint-exchange category. Default is to fetch only the latest topics.",
) """
parser.add_argument(
    "--debug",
    action="store_true",
    help="Enable debug logging.",
)
""" parser.add_argument(
    "--db-local",
    action="store_true",
    help="Use a local database file instead of the default remote URL.",
) """
args = parser.parse_args()

# Configure logging
logging.basicConfig(
    filename="main.log", level=logging.DEBUG if args.debug else logging.INFO
)


def main():

    try:
        db = Database(local=True, drop_existing_tables=False)
        
        with db.engine.connect() as connection:
            inspector = inspect(connection)
            columns = [col["name"] for col in inspector.get_columns("blueprints")]
            if "topic_keywords" not in columns:
                connection.execute(
                    text("ALTER TABLE blueprints ADD COLUMN topic_keywords JSON")
                )
            if "keywords_yake" not in columns:
                connection.execute(
                    text("ALTER TABLE blueprints ADD COLUMN keywords_yake JSON")
                )
            if "keywords_tfidf" not in columns:
                connection.execute(
                    text("ALTER TABLE blueprints ADD COLUMN keywords_tfidf JSON")
                )
    
        update_blueprint_keywords(db)
        update_blueprint_keywords_yake(db)
        update_blueprint_keywords_tfidf(db)
    except Exception as e:
        logging.error(str(e))


if __name__ == "__main__":
    main()
