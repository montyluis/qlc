# -*- coding: utf-8 -*-

# requires that regex package is installed to get "\X" Unicode grapheme match
# http://pypi.python.org/pypi/regex/

import regex
import codecs
import unicodedata

def stopwords_from_file(stopwords_filepath = "data/stopwords/spa.txt"):
    stopwords = codecs.open(stopwords_filepath, "r", "utf-8")
    ret = set()
    for line in stopwords:
        word = line.rstrip("\n")
        word = regex.sub(" *\|.*$", "", word)
        if regex.search("[^\s]", word):
            word = unicodedata.normalize("NFD", word)
            ret.add(word)
    return ret

def remove_stopwords(phrase, stopwords):
    re_stopwords = regex.compile(r"\b(?:{0})\b".format( "|".join(stopwords).encode("utf-8") ))
    re_spaces = regex.compile(" +")

    phrase = phrase.strip(" ")
    w_stop = phrase
    if " " in w_stop:
        w_stop = re_stopwords.sub("", w_stop)
        w_stop = w_stop.strip(" ")
        w_stop = re_spaces.sub(" ", w_stop)
    return w_stop
        
def stem_phrase(phrase, stemmer, split_multiwords=False):
    if " " in phrase:
        if split_multiwords:
            ret = []
            for w in phrase.split(" "):
                ret.append(stemmer.stem(w))
            return ret
        else:
            return([])
    elif len(phrase) > 0:
        return [stemmer.stem(phrase)]
    else:
        return([])

def parse_graphemes(string):
    grapheme_pattern = regex.compile("\X", regex.UNICODE)
    return regex.split(grapheme_pattern, string)
