from copy import deepcopy


def replace_input_tags(blueprint_dict: dict) -> dict:
    """Place input tags defined in the top into where they were used."""
    if "input" in blueprint_dict["blueprint"]:
        inputs = blueprint_dict["blueprint"]["input"]
        replace_input_references(blueprint_dict, inputs)
    blueprint_dict = remove_declaration(blueprint_dict)
    return blueprint_dict


def replace_input_references(node, inputs):
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, dict) and "!input" in value:
                input_key = value["!input"]
                if input_key in inputs:
                    node[key] = deepcopy(inputs[input_key])
            else:
                replace_input_references(value, inputs)
    elif isinstance(node, list):
        for i, item in enumerate(node):
            if isinstance(item, dict) and "!input" in item:
                input_key = item["!input"]
                if input_key in inputs:
                    node[i] = deepcopy(inputs[input_key])
            else:
                replace_input_references(item, inputs)


def remove_declaration(blueprint_dict: dict) -> dict:
    """Remove blocks other than trigger, condition, and action."""
    for key in list(blueprint_dict.keys()):
        if key == "blueprint":
            for prop in list(blueprint_dict[key].keys()):
                if prop in ["input", "source_url", "domain"]:
                    del blueprint_dict[key][prop]
        elif key not in ["trigger", "condition", "action"]:
            del blueprint_dict[key]
    return blueprint_dict
