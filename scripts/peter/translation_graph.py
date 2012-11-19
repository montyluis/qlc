# -*- coding: utf-8 -*-

import codecs, unicodedata
import collections
import re
import copy

from qlc.corpusreader import CorpusReaderDict
from qlc.translationgraph import read, write
import qlc.utils

from nltk.stem.snowball import SpanishStemmer

import networkx

cr = CorpusReaderDict("/home/pbouda/Projects/git-github/qlc/data")

dictdata_ids = cr.dictdata_ids_for_component("Witotoan")
re_quotes = re.compile('"')

print(dictdata_ids)

graphs = list()
for dictdata_id in dictdata_ids:
    gr = networkx.Graph()

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
        gr.add_node(head_with_source, attr_dict={ "lang": language_iso, "source": bibtex_key })
        gr.add_node(translation, attr_dict={ "lang": "spa" })
        gr.add_edge(head_with_source, translation)
    graphs.append(gr)

print(networkx.algorithms.components.number_connected_components(graphs[0]))

combined_graph = copy.deepcopy(graphs[0])
for gr in graphs[1:]:
    for node in gr:
        combined_graph.add_node(node, gr.node[node])
    for n1, n2 in gr.edges_iter():
        combined_graph.add_edge(n1, n2, gr.edge[n1][n2])

print(networkx.algorithms.components.number_connected_components(combined_graph))

combined_graph_stemmed = copy.deepcopy(combined_graph)

stemmer = SpanishStemmer(True)
split_multiwords = False

stopwords = qlc.utils.stopwords_from_file("../../src/qlc/data/stopwords/spa.txt")

for node in combined_graph.nodes():
    if "lang" in combined_graph.node[node] and combined_graph.node[node]["lang"] == "spa":
        phrase_without_stopwords = qlc.utils.remove_stopwords(node, stopwords)
        phrase_stems = qlc.utils.stem_phrase(phrase_without_stopwords, stemmer, split_multiwords)
        for stem in phrase_stems:
            stem = stem + "|stem"
            combined_graph_stemmed.add_node(stem, is_stem=True)
            combined_graph_stemmed.add_edge(stem, node)

print(networkx.algorithms.components.number_connected_components(combined_graph_stemmed))

OUT = codecs.open("translation_graph_stemmed.dot", "w", "utf-8")
OUT.write(write(combined_graph_stemmed))
OUT.close()

matrix = {}
sources = set()
for node in combined_graph_stemmed:
    if "is_stem" in combined_graph_stemmed.node[node] and combined_graph_stemmed.node[node]["is_stem"]:
        spanish_nodes = [n for n in combined_graph_stemmed[node] if "lang" in combined_graph_stemmed.node[n] and combined_graph_stemmed.node[n]["lang"] == "spa"]
        head_nodes = []
        for sp in spanish_nodes:
            head_nodes += [n for n in combined_graph_stemmed[sp] if ("lang" not in combined_graph_stemmed.node[n] or combined_graph_stemmed.node[n]["lang"] != "spa") and ("is_stem" not in combined_graph_stemmed.node[n] or not combined_graph_stemmed.node[n]["is_stem"])]
        head_nodes = set(head_nodes)

        heads = collections.defaultdict(list)
        for head in head_nodes:
            (head, source) = head.split("|")
            sources.add(source)
            heads[source].append(head)
        matrix["|".join(sorted(spanish_nodes))] = heads

OUT = codecs.open("translation_matrix.csv", "w", "utf-8")
sorted_sources = sorted(sources)
OUT.write("{0}\t{1}\n".format("spa", "\t".join(sorted_sources)))
for spanish in sorted(matrix):
    OUT.write(spanish)
    OUT.write("\t")
    sources_heads = []
    for source in sorted(sources):
        heads = [h for h in matrix[spanish][source]]
        sources_heads.append("|".join(sorted(heads)))
    OUT.write("\t".join(sources_heads))
    OUT.write("\n")
OUT.close()
