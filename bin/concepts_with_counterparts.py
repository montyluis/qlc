# -*- coding: utf-8 -*-
#!/usr/bin/env python3
#-----------------------------------------------------------------------------
# Copyright (c) 2011, Quantitative Language Comparison Team
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file COPYING.txt, distributed with this software.
#-----------------------------------------------------------------------------

import sys
import os
import codecs
import collections

from qlc.corpusreader import CorpusReaderWordlist

def main(argv):

    if len(argv) < 2:
        print("call: concepts_with_counterparts.py data_path [(bibtex_key|component)]")
        exit(1)

    cr = CorpusReaderWordlist(argv[1])
    print("Data loaded", file=sys.stderr)
    
    dictdata_ids = []    
    if len(argv) == 3:
        wordlistdata_ids = cr.wordlistdata_ids_for_bibtex_key(argv[2])
        if len(wordlistdata_ids) == 0:
            wordlistdata_ids = cr.wordlistdata_ids_for_component(argv[2])
            if len(wordlistdata_ids) == 0:
                print("did not find any dictionary data for the bibtex_key or component {0}.".format(argv[2]), file=sys.stderr)
                sys.exit(1)
    else:
        wordlistdata_ids = cr.wordlistdata_string_ids
        
    bibtex_keys = collections.defaultdict(list)
    for wid in wordlistdata_ids:
        wordlistdata_string = cr.wordlistdata_string_ids[wid]
        bibtex_key = wordlistdata_string.split("_")[0]
        bibtex_keys[bibtex_key].append(wid)
        
    for bibtex_key in bibtex_keys:
    
        print("Writing data for wordlistdata bibtex key {0}".format(bibtex_key), file=sys.stderr)

        output = codecs.open("concepts_with_counterparts_%s.txt" % bibtex_key, "w", "utf-8")
        output.write("COUNTERPART\tCONCEPT\tLANGUAGE_BOOKNAME\tLANGUAGE_CODE\tBIBTEX_KEY\n")

        for wordlistdata_id in bibtex_keys[bibtex_key]:
            #heads_with_translations = cr.heads_with_translations_for_dictdata_id(dictdata_id)
            language_bookname = cr.get_language_bookname_for_wordlistdata_id(wordlistdata_id)
            language_code = cr.get_language_code_for_wordlistdata_id(wordlistdata_id)
            
            for concept, counterpart in cr.data(wordlistdata_id):
                output.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format(counterpart, concept, language_bookname, language_code, bibtex_key))
            
        output.close()

        if os.path.getsize("concepts_with_counterparts_%s.txt" % bibtex_key) == 0:
            os.remove("concepts_with_counterparts_%s.txt" % bibtex_key)


if __name__ == "__main__":
    main(sys.argv)
