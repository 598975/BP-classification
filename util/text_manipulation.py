import re
import yaml
import logging
from bs4 import BeautifulSoup
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import json

nltk.download("wordnet")


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
    """Recursively retrieves leaf values from nested data structures."""
    if isinstance(data, dict):
        for value in data.values():
            yield from get_leaf_values(value)
    elif isinstance(data, list):
        for item in data:
            yield from get_leaf_values(item)
    else:
        yield data


def remove_html(text):
    soup = BeautifulSoup(text, "html.parser")

    for code_tag in soup.find_all("code", {"class": ["lang-auto", "lang-yaml"]}):
        code_tag.decompose()

    for a_tag in soup.find_all("a"):
        a_tag.decompose()

    return soup.get_text().replace("\n", " ").strip()


def preprocess(text):
    text = remove_html(text)
    text = text.lower()
    text = re.sub(r"’", r"'", text)
    return text


def tfidf_preprocessing(text, ignorable_words: list[str] | str | None = None):
    if ignorable_words is None:
        ignorable_words = []
    elif not isinstance(ignorable_words, list):
        ignorable_words = [ignorable_words]
    ignorable_words = ignorable_words + ["blueprint", "automation", "entity", "work"]
    text = remove_html(text)
    lemmatizer = WordNetLemmatizer()
    ignorable_words = [lemmatizer.lemmatize(w.lower()) for w in ignorable_words]
    text = text.lower()
    text = re.sub(r"’", r"'", text)
    text = re.sub(r"[^\w'\s]", "", text)
    text = re.sub(r"\b\d+\b", "", text)
    text = text.split()
    text = [
        lemmatizer.lemmatize(word)
        for word in text
        if word not in stopwords.words("english")
    ]
    text = " ".join(text)
    # text = re.sub("|".join(ignorable_words), "", text, flags=re.IGNORECASE)
    safe_tokens = [re.escape(w) for w in ignorable_words if w]
    if safe_tokens:
        pattern = "|".join(safe_tokens)
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    return text


def keywords_remove_input(kwd_dict: dict[str, int] | str) -> list[str] | None:
    if isinstance(kwd_dict, str):
        kwd_dict = json.loads(kwd_dict)

    kwd_list = list(kwd_dict.keys())
    if kwd_list.__len__() < 1:
        return None

    kwds = []

    for kwd in kwd_list:
        in_out = re.search(r"(input__|output__)(input_|output_)?", kwd)
        kwd = kwd.removeprefix(in_out.group()) if in_out else kwd
        kwds.append(kwd)
    return kwds
