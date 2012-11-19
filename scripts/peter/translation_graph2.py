import codecs, unicodedata
import collections
import re
import copy
import itertools

from qlc.corpusreader import CorpusReaderDict
from qlc.translationgraph import read, write
import qlc.utils

from nltk.stem.snowball import SpanishStemmer

import networkx

stopwords = qlc.utils.stopwords_from_file("src/qlc/data/stopwords/spa.txt")
stemmer = SpanishStemmer(False)

cr = CorpusReaderDict("data")
dictdata_ids = cr.dictdata_ids_for_component("Witotoan")
re_quotes = re.compile('"') 
graphs = dict()
heads = dict()
translations = dict()
for dictdata_id in dictdata_ids:
    gr = networkx.Graph()
    h = set()
    t = set()

    src_language_iso = cr.src_language_iso_for_dictdata_id(dictdata_id)
    tgt_language_iso = cr.tgt_language_iso_for_dictdata_id(dictdata_id)
    if src_language_iso != 'spa' and tgt_language_iso != 'spa':
        continue

    language_iso = None
    if tgt_language_iso == 'spa':
        language_iso = src_language_iso
    else:
        language_iso = tgt_language_iso

    dictdata_string = cr.dictdata_string_id_for_dictata_id(dictdata_id)
    bibtex_key = dictdata_string.split("_")[0]

    for head, translation in cr.heads_with_translations_for_dictdata_id(dictdata_id):
        if src_language_iso == 'spa':
            (head, translation) = (translation, head)

        head_with_source = re_quotes.sub('', "{0}|{1}".format(head, bibtex_key))
        translation = re_quotes.sub('', translation)
        translation = qlc.utils.remove_stopwords(translation, stopwords)
        translation = qlc.utils.stem_phrase(translation, stemmer, False)
        if len(translation) > 0:
            if len(translation) > 1:
                print("error")
                print(translation)
            translation = translation[0]
            gr.add_node(head_with_source, attr_dict={ "lang": language_iso, "source": bibtex_key })
            gr.add_node(translation, attr_dict={ "lang": "spa" })
            gr.add_edge(head_with_source, translation)
            h.add(head_with_source)
            t.add(translation)
    graphs[language_iso] = gr
    heads[language_iso] = h
    translations[language_iso] = t
    
languages = sorted(heads.keys())

##########################################################################
distances = dict()
for lang1, lang2 in itertools.combinations(languages, 2):
    combined_graph = copy.deepcopy(graphs[lang1])
    for n in graphs[lang2]:
        combined_graph.add_node(n, graphs[lang2][n])
    for n1, n2 in graphs[lang2].edges_iter():
        combined_graph.add_edge(n1, n2, graphs[lang2].edge[n1][n2])

    distance = 0
    for h1 in heads[lang1]:
        for h2 in heads[lang2]:
            b_single = 0
            b_shared = 0
            neighbours = combined_graph[h1]
            for n in neighbours:
                n_2 = combined_graph[n]
                if h2 in n_2:
                    b_shared += 1
                else:
                    b_single += 1
            distance += float(b_shared) / (float(b_single) + float(b_shared))
    distance = distance / (len(heads[lang1]) * len(heads[lang2]))
    distances[lang1, lang2] = distance

    
import scipy
m = scipy.zeros([len(languages), len(languages)])

for i in range(len(languages)):
    for j in range(len(languages)):

        if i == j:
            d = 0
        else:
            if (languages[i], languages[j]) in distances:
                d = distances[languages[i], languages[j]] * 100
            else:
                d = distances[languages[j], languages[i]] * 100
            d = 1.0 - d
        m[i, j] = d

import matplotlib.pyplot as plt
import scipy.cluster.hierarchy as sch       
Y = sch.linkage(m, method='centroid')
Z = sch.dendrogram(Y)

#plt.show()

##########################################################################

import scipy
import math

m = scipy.zeros([len(languages), len(languages)])
distances = dict()
linked = collections.defaultdict(int)
for lang1, lang2 in itertools.combinations(languages, 2):
    i = languages.index(lang1)
    j = languages.index(lang2)
    N = float(len(translations[lang1].union(translations[lang2])))
    combined_graph = copy.deepcopy(graphs[lang1])
    for n in graphs[lang2]:
        combined_graph.add_node(n, graphs[lang2][n])
    for n1, n2 in graphs[lang2].edges_iter():
        combined_graph.add_edge(n1, n2, graphs[lang2].edge[n1][n2])

    MI = 0.0
    H_X = 0.0
    H_Y = 0.0
    for h1 in heads[lang1]:
        for h2 in heads[lang2]:
            
            neighbors1 = set(combined_graph[h1].keys())
            neighbors2 = set(combined_graph[h2].keys())
            
            P_X_Y = float(len(neighbors1.intersection(neighbors2)))
            
            if P_X_Y > 0:
                linked[lang1, lang2] += 1
                P_X = float(len(neighbors1))
                P_Y = float(len(neighbors2))
                I = (P_X_Y / N) * math.log( P_X_Y / (P_X * P_Y * N), 2 )
                H_X += (-P_X) * math.log(P_X, 2)
                H_Y += (-P_Y) * math.log(P_Y, 2)
                MI += I
    D = H_X + H_Y - (2*MI)
    m[i, j] = D
    m[j, i] = D
    distances[lang1, lang2] = D
