import os
KB_CONST_PATH = os.path.dirname(__file__)

KB_ZONE_CI_PATH = os.path.join(KB_CONST_PATH, "kb_ci_zone_parsing.json")

STOPWORDS = {"fr": os.path.join(KB_CONST_PATH, "stopwords-fr.txt"),
             "en": os.path.join(KB_CONST_PATH, "stopwords-en.txt")}