# -*- coding: utf-8 -*-

import pickle
from nltk.stem.snowball import SpanishStemmer

stemmer = SpanishStemmer()

f = open("sentences_for_stems.pickle", "rb")
sentences_for_stem = pickle.load(f)
f.close()

f = open("docs_for_stems.pickle", "rb")
docs_for_stem = pickle.load(f)
f.close()

stem1 = stemmer.stem("continua")
stem2 = stemmer.stem("figura")

print sentences_for_stem[stem1]
print sentences_for_stem[stem2]
print len(sentences_for_stem)
print len(docs_for_stem)

print sentences_for_stem[stem1] & sentences_for_stem[stem2]
