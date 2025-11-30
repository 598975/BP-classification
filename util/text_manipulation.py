import re
import yaml
import logging
from bs4 import BeautifulSoup
import nltk
from nltk.stem import WordNetLemmatizer
nltk.download("wordnet")
from nltk.corpus import stopwords
from tqdm import tqdm

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

def remove_html(text):
    soup = BeautifulSoup(text, 'html.parser')
    
    for code_tag in soup.find_all('code', {'class':['lang-auto','lang-yaml']}):
        code_tag.decompose()
    
    for a_tag in soup.find_all('a'):
        a_tag.decompose()
    
    return soup.get_text().replace('\n', ' ').strip()
 
def yake_preprocessing(text):
    text = remove_html(text)
    text = text.lower()
    text = re.sub(r'’', r"'", text)
    text = re.sub(r"[^\w'\s]", '', text)
    return text

def get_corpus(texts: list[str], stopwords_list: list[str] | None = None):
    corpus = []
    for topic in tqdm(_topic_post_df["topic_id"].unique(), desc="Preprocessing and grouping by topic"):
        topic_subset = _topic_post_df[_topic_post_df["topic_id"] == topic]
        texts = topic_subset["cooked"].tolist()
        combined_text = " ".join([tfidf_preprocessing(text, stopwords_list) for text in texts])
        corpus.append(combined_text)
    return corpus

def tfidf_preprocessing(text, ignorable_words : list[str] | str | None = None):
    if ignorable_words is None:
        ignorable_words = []
    elif not isinstance(ignorable_words, list):
        ignorable_words = [ignorable_words]
    ignorable_words = ignorable_words + ["blueprint", "automation", "entity", "work"]
    text = remove_html(text)
    lemmatizer = WordNetLemmatizer()
    text = text.lower()
    text = re.sub(r"’", r"'", text)
    text = re.sub(r"[^\w'\s]", "", text)
    text = text.split()
    text = [lemmatizer.lemmatize(word) for word in text if word not in stopwords.words("english")]
    text = " ".join(text)
    """ text = re.sub("|".join(ignorable_words), "", text, flags=re.IGNORECASE) """
    safe_tokens = [re.escape(w) for w in ignorable_words if w]
    if safe_tokens:
        pattern = "|".join(safe_tokens)
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    return text