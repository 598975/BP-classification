import yaml
import logging


def input_constructor(loader, node):
    # Get the value from the node
    value = loader.construct_scalar(node)
    return {"!input": value}


# Register the custom constructor with the SafeLoader
yaml.SafeLoader.add_constructor("!input", input_constructor)


def parse_yaml(text) -> dict | None:
    try:
        # Attempt to load the text as YAML
        return yaml.load(text, Loader=yaml.SafeLoader)
    except yaml.YAMLError as e:
        # Log the error
        logging.debug("Invalid YAML: " + str(e))
        return None


def normalize_text(domain: str):
    return domain.lower().replace("-", "_").replace("/", "_").replace(" ", "_")

def get_leaf_values(data):
    """Recursively retrieves leaf values, which in HA blueprints are user-defined strings and not predetermined YAML tags."""
    if isinstance(data, dict):
        for value in data.values():
            yield from get_leaf_values(value)
    elif isinstance(data, list):
        for item in data:
            yield from get_leaf_values(item)
    else:
        yield data
