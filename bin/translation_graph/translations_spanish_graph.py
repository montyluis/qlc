# -*- coding: utf8 -*-
#!/usr/bin/env python3

"""
This script outputs a dot files for each dictionary. The dot file each
contain a graph, where each spanish translation is connected to each
head word of the dictionary's native language. The spanish translation are
not processed. The head words' strings are each connected to a string with
the source ID. This later helps to distinguish head words from different sources
that are represented by the same string.
"""

import sys, codecs, collections, unicodedata, re

from qlc.corpusreader import CorpusReaderDict
from networkx import Graph
from qlc.translationgraph import write

re_quotes = re.compile('"')

def escape_string(s):
    ret = re_quotes.sub('', s)
    #if not ret.startswith('"') or not ret.endswith('"'):
    #    ret = '"{0}"'.format(ret)
    return ret

def main(argv):
    
    if len(argv) < 3:
        print("call: translations_spanish_graph.py data_path (bibtex_key|component)")
        sys.exit(1)

    cr = CorpusReaderDict(argv[1])

    dictdata_ids = cr.dictdata_ids_for_bibtex_key(argv[2])
    if len(dictdata_ids) == 0:
        dictdata_ids = cr.dictdata_ids_for_component(argv[2])
        if len(dictdata_ids) == 0:
            print("did not find any dictionary data for the bibtex_key or component {0}.".format(argv[2]))
            sys.exit(1)
        

    for dictdata_id in dictdata_ids:
        gr = Graph()
        src_language_iso = cr.src_languages_iso_for_dictdata_id(dictdata_id)
        tgt_language_iso = cr.tgt_languages_iso_for_dictdata_id(dictdata_id)
        if (src_language_iso != ['spa']) and (tgt_language_iso != ['spa']):
            continue
        
        if (len(src_language_iso) > 1) or (len(tgt_language_iso) > 1):
            continue
        
        language_iso = None
        if tgt_language_iso == [ 'spa' ]:
            language_iso = src_language_iso[0]
        else:
            language_iso = tgt_language_iso[0]
                        
        dictdata_string = cr.dictdata_string_id_for_dictata_id(dictdata_id)
        bibtex_key = dictdata_string.split("_")[0]

        for head, translation in cr.heads_with_translations_for_dictdata_id(dictdata_id):
            if src_language_iso == [ 'spa' ]:
                (head, translation) = (translation, head)
                
            head_with_source = escape_string("{0}|{1}".format(head, bibtex_key))
            translation = escape_string(translation)
            
            #translation_with_language = "{0}|{1}".format(translation, language_iso)
            
            #if head_with_source not in gr:
            gr.add_node(head_with_source, attr_dict={ "lang": language_iso, "source": bibtex_key })
            
            #if translation not in gr:
            gr.add_node(translation, attr_dict={ "lang": "spa" })
                
            #if not gr.has_edge((head_with_source, translation)):
            gr.add_edge(head_with_source, translation)

        output = codecs.open("{0}.dot".format(dictdata_string), "w", "utf-8")
        output.write(write(gr))
        output.close()

if __name__ == "__main__":
    main(sys.argv)
