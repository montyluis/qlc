# -*- coding: utf8 -*-
#!/usr/bin/env python3

"""
This script akes as input one dot files and output a corresponding dot file
which has additional edges between spanish translations. The additional edges
connect translations that have equal stems. Before the comparison stop words
will be removed from each translation. If you add the argument
"splitmultiwords", translations with more than one word will be splitted and
each word of the translation will be compared with each word of each other
translation.
"""

import sys, codecs, collections, unicodedata
import regex as re
import qlc.utils

from pygraph.classes.graph import graph
from pygraph.algorithms.traversal import traversal
from qlc.translationgraph import write, read

# snowball stemmer: http://snowball.tartarus.org/download.php

import Stemmer

def main(argv):
    
    if len(argv) < 3:
        print("call: translations_spanish_graph_connectstemswithoutstopwords.py graph_file_in.dot graph_file_out.dot [splitmultiwords]")
        sys.exit(1)
        
    split_multiwords = False
    if len(argv) == 4 and argv[3] == "splitmultiwords":
        print("Will split multiwords.")
        split_multiwords = True

    IN = codecs.open(sys.argv[1], "r", "utf-8")
    gr = read(IN.read())
    IN.close()
 
    print("Parse finished.", file=sys.stderr)
    nodes = gr.nodes()

    stemmer = Stemmer.Stemmer('spanish')
    stopwords = qlc.utils.stopwords_from_file("data/stopwords/spa.txt")

    i = 0    
    for n in nodes:
        if "lang" in gr.node[n] and gr.node[n]["lang"] == "spa":
            phrase_without_stopwords = qlc.utils.remove_stopwords(n, stopwords)
            phrase_stems = qlc.utils.stem_phrase(phrase_without_stopwords, stemmer, split_multiwords)
            for stem in phrase_stems:
                stem = stem + "|stem"
                gr.add_node(stem, is_stem=True)
                gr.add_edge(stem, n)
    
    OUT = codecs.open(sys.argv[2], "w", "utf-8")
    OUT.write(write(gr))
    OUT.close()

if __name__ == "__main__":
    main(sys.argv)