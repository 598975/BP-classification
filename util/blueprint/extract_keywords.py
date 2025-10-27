def extract_keywords(bp_dict) -> dict:
    keywords = {"trigger": [], "condition": [], "action": []}

    def extract_attr_rec(bp_dict, section=None):
        if isinstance(bp_dict, dict):
            for key, value in bp_dict.items():
                # Check if the current key indicates a new section and update accordingly
                if key in ["trigger", "condition", "action"]:
                    section = key
                elif key in ["wait_for_trigger", "data"]:
                    section = "condition"
                elif key == "variables":
                    section = ""
                # Check if the key is one of the attributes we're interested in
                elif key in ["integration", "domain", "device_class"] and section:
                    # Extend or append the value(s) to the respective list
                    if isinstance(value, list):
                        keywords[section].extend(value)
                    else:
                        keywords[section].append(value)
                # Recurse into dictionaries and lists, maintaining the current section
                if isinstance(value, (dict, list)):
                    extract_attr_rec(value, section)
        elif isinstance(bp_dict, list):
            for item in bp_dict:
                extract_attr_rec(item, section)

    # Start the recursive extraction
    extract_attr_rec(bp_dict)
    return keywords
