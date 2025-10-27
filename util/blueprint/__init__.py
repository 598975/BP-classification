import voluptuous as vol
from logging import debug
from .schema import BLUEPRINT_SCHEMA
from .expand import replace_input_tags
from .extract_keywords import extract_keywords


def validate_blueprint(blueprint_dict):
    """Validate a blueprint against the schema."""
    try:
        BLUEPRINT_SCHEMA(blueprint_dict)
        return True
    except vol.Invalid as e:
        debug("Invalid blueprint: " + str(e))
        return False


def expand_blueprint(blueprint_dict):
    """Place input tags defined in the top into where they were used."""
    try:
        return replace_input_tags(blueprint_dict)
    except Exception as e:
        debug("Error replacing input references: " + str(e))
        raise e
