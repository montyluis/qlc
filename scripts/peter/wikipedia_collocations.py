# -*- coding: utf-8 -*-

import fileinput
import nltk
import regex
import collections, codecs
from nltk.stem.snowball import SpanishStemmer
import unicodedata
import pickle

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

tokenizer = nltk.load("tokenizers/punkt/spanish.pickle")
stopwords = stopwords_from_file("../../src/qlc/data/stopwords/spa.txt")
stemmer = SpanishStemmer()

doc = ""
doc_id = 0
sentence_id = 0

sentences_for_stem = collections.defaultdict(set)
docs_for_stem = collections.defaultdict(set)

for l in fileinput.input("/Users/ramon/qlc-github/data/eswiki/AA/wiki00"):
    l = l.strip()
    l = l.decode("utf-8")
    l = unicodedata.normalize("NFD", l)
    
    if l.startswith("</doc>"):
        sentences = tokenizer.tokenize(doc)
        for s in sentences:
            s = regex.sub(u"[^\p{L}\p{M}]", " ", s)
            s = s.lower()
            for w in s.split():
                if not w in stopwords:
                    stem = w
                    if len(w) > 3:
                        stem = stemmer.stem(w)
                    sentences_for_stem[stem].add(sentence_id)
                    docs_for_stem[stem].add(doc_id)
            sentence_id += 1
        doc = ""
        doc_id += 1
            
    elif not l.startswith("<doc"):
        l = regex.sub("</?a[^>]*>", "", l)
        doc += l + " "
    
    #if doc_id > 500:
    #    break
    
stem1 = stemmer.stem("continua")
stem2 = stemmer.stem("figura")

print sentences_for_stem[stem1]
print sentences_for_stem[stem2]
print len(sentences_for_stem)
print len(docs_for_stem)

print sentences_for_stem[stem1] & sentences_for_stem[stem2]

f = open("sentences_for_stems.pickle", "wb")
pickle.dump(sentences_for_stem, f)
f.close()

f = open("docs_for_stems.pickle", "wb")
pickle.dump(docs_for_stem, f)
f.close()