from db.database import Database
from db.keyword_extraction import (
    update_blueprint_keywords,
    update_blueprint_keywords_yake,
)
from util.filtering import filter_blueprints
import logging
import argparse
from sqlalchemy.sql import text
from sqlalchemy import inspect

parser = argparse.ArgumentParser(description="Classify blueprints.")
""" parser.add_argument(
    "--fetch-new",
    action="store_true",
    help="Fetch and store the new topics in the blueprint-exchange category.",
)"""
parser.add_argument(
    "--update_keywords",
    action="store_true",
    help="Update keywords in database.",
)
parser.add_argument(
    "--debug",
    action="store_true",
    help="Enable debug logging.",
)
parser.add_argument(
    "--filter_bps",
    action="store_true",
    help="Filter out blueprints based on language and similarity.",
)
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

        if args.update_keywords:
            update_blueprint_keywords(db)
            update_blueprint_keywords_yake(db)

        if args.filter_bps:
            filter_blueprints(db)

    except Exception as e:
        logging.error(str(e))


if __name__ == "__main__":
    main()
