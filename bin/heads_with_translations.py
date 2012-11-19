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

from qlc.corpusreader import CorpusReaderDict

def main(argv):

    if len(argv) < 2:
        print("call: heads_with_translations.py data_path [(bibtex_key|component)]")
        exit(1)

    cr = CorpusReaderDict(argv[1])
    print("Data loaded", file=sys.stderr)
    
    dictdata_ids = []    
    if len(argv) == 3:
        dictdata_ids = cr.dictdata_ids_for_bibtex_key(argv[2])
        if len(dictdata_ids) == 0:
            dictdata_ids = cr.dictdata_ids_for_component(argv[2])
            if len(dictdata_ids) == 0:
                print("did not find any dictionary data for the bibtex_key or component {0}.".format(argv[2]))
                sys.exit(1)
    else:
        dictdata_ids = cr.dictdata_string_ids
        
    
    for dictdata_id in dictdata_ids:
        #heads_with_translations = cr.heads_with_translations_for_dictdata_id(dictdata_id)
        dictdata_string = cr.dictdata_string_id_for_dictata_id(dictdata_id)
        print("Writing data for dictdata string ID {0}".format(dictdata_string), file=sys.stderr)

        output = codecs.open("heads_with_translations_%s.txt" % dictdata_string, "w", "utf-8")
        
        for head, translation in cr.data(dictdata_id):
            output.write("%s\t%s\n" % (head, translation))
        
        output.close()

        if os.path.getsize("heads_with_translations_%s.txt" % dictdata_string) == 0:
            os.remove("heads_with_translations_%s.txt" % dictdata_string)

if __name__ == "__main__":
    main(sys.argv)
