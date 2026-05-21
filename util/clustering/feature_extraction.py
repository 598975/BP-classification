import json
import os
import pickle
import random
import re
from difflib import SequenceMatcher
from pathlib import Path

import numpy as np
import pandas as pd
from nltk.stem import WordNetLemmatizer
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import normalize

random.seed(42)
np.random.seed(42)
os.environ["PYTHONHASHSEED"] = "42"


def _process_bp_keywords(kwd_dict: dict[str, int] | str) -> list[str] | None:
    if isinstance(kwd_dict, str):
        kwd_dict = json.loads(kwd_dict)

    kwd_list = list(kwd_dict.keys())
    if kwd_list.__len__() < 1:
        return None

    kwds = []

    for kwd in kwd_list:
        in_out = re.search(r"(input__|output__)(input_|output_)?", kwd)
        kwd = kwd.removeprefix(in_out.group()) if in_out else kwd
        in_out = in_out.group(1) if in_out else None
        kwds.append(in_out + kwd if in_out else kwd)
    return kwds


def _normalize_feature_token(token: str) -> str:
    token = token.lower().strip()
    token = re.sub(r"[^\wÆØÅæøå]+", " ", token)
    token = re.sub(r"\s+", "_", token).strip()
    lemmatizer = WordNetLemmatizer()
    token = lemmatizer.lemmatize(token)
    return token


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _normalize_yake_features(
    features: list[str], threshold: float = 0.88, min_len: int = 4
) -> list[str]:
    """
    Canonicalize YAKE keywords using fuzzy matching and simple containment rules.
    Keeps original order but maps near-duplicates to a shared canonical token.
    """
    canonicals: list[str] = []
    canonical_map: dict[str, str] = {}
    for raw in features:
        norm = _normalize_feature_token(raw)
        if not norm:
            continue
        if norm in canonical_map:
            canonical = canonical_map[norm]
            canonicals.append(canonical)
            continue
        canonical = norm
        for existing in set(canonical_map.values()):
            if min(len(norm), len(existing)) >= min_len and (
                norm in existing or existing in norm
            ):
                canonical = existing if len(existing) <= len(norm) else norm
                break
            if _similarity(norm, existing) >= threshold:
                canonical = existing
                break
        canonical_map[norm] = canonical
        canonicals.append(canonical)
    return canonicals


def _create_feature_vector(row, max_features, max_keywords, max_tags) -> list[str]:
    padding = "<PAD>"

    yake_features = json.loads(row["keywords_yake"])
    yake_features = _normalize_yake_features(yake_features)
    combined_features = list(yake_features)
    ext_kw = _process_bp_keywords(row["extracted_keywords"])
    if ext_kw:
        combined_features.extend(ext_kw)

    kw_n = max_keywords - len(ext_kw) if ext_kw else max_keywords
    combined_features += [padding] * kw_n

    tags = row["tags"]
    tag_n = max_tags - len(tags)
    combined_features.extend(tags)
    combined_features += [padding] * tag_n

    return combined_features


def extract_features(bp_df):
    max_keywords = (
        bp_df["extracted_keywords"]
        .apply(lambda x: len(json.loads(x)) if isinstance(x, str) else 0)
        .max()
    )
    max_tags = bp_df["tags"].apply(lambda x: len(x) if isinstance(x, list) else 0).max()
    max_yake = 4
    max_features = max_keywords + max_tags + max_yake

    bp_df["features"] = bp_df.apply(
        lambda row: _create_feature_vector(
            row, max_features=max_features, max_keywords=max_keywords, max_tags=max_tags
        ),
        axis=1,
    )
    bp_df["features"] = bp_df["features"].apply(lambda x: " ".join(x))

    cache_file = Path("output") / "embeddings_cache.pkl"
    if os.path.exists(cache_file):
        print("Loading cached embeddings...")
        with open(cache_file, "rb") as f:
            cached_data = pickle.load(f)
            bp_df["embeddings"] = cached_data["embeddings"]
            X = cached_data["X"]
            X_normalized = cached_data["X_normalized"]
    else:
        print("Computing embeddings...")
        model = SentenceTransformer("all-MiniLM-L6-v2")
        # Process in deterministic single-threaded mode
        embeddings_list = []
        for text in bp_df["features"]:
            embedding = model.encode(
                text, show_progress_bar=False, convert_to_numpy=True
            )
            embeddings_list.append(embedding)

        bp_df["embeddings"] = embeddings_list
        X = np.vstack(bp_df["embeddings"].values)
        X_normalized = normalize(X, norm="l2")

        # Cache for future runs
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, "wb") as f:
            pickle.dump(
                {"embeddings": embeddings_list, "X": X, "X_normalized": X_normalized}, f
            )
        print("Embeddings cached.")

    return bp_df, X_normalized
