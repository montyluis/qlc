# -*- coding: utf-8 -*-

"""
Basic script to generate an initial orthography profile from a dictionary 
resource
"""



import sys, collections
import qlc.utils
from qlc.corpusreader import CorpusReaderDict
from datetime import datetime


def main(argv):
    # check for the right number of command line arguments
    if len(argv) < 3:
        print()
        print("Call: create_initial_orthography_profile.py data_path data_source")
        print()
        print("python create_initial_orthography_profile.py data/csv/ thiesen1998")
        sys.exit(1)

    data_path = sys.argv[1]
    data_source = sys.argv[2]

    orthography_profile = open(data_source+"_initial_profile.txt", "w") # output file
    cr = CorpusReaderDict(data_path) 
    dictdata_ids = cr.dictdata_ids_for_bibtex_key(data_source)

    # make sure the resource is in the data
    if len(dictdata_ids) == 0:
        print("There is no dictionary source for the data source you provided: "+data_source)
        sys.exit(1)


    grapheme_frequency_dict = collections.defaultdict(int)
    grapheme_count = 0.0

    for dictdata_id in dictdata_ids:
        for head, translation in cr.heads_with_translations_for_dictdata_id(dictdata_id):
            graphemes = qlc.utils.parseGraphemes(head)
            for grapheme in graphemes:
                grapheme_count += 1
                grapheme_frequency_dict[grapheme] += 1

    header = "grapheme"+"\t"+"count"+"\t"+"total frequency"
    print(header)
    orthography_profile.write(header+"\n")
    for k, v in grapheme_frequency_dict.items():
        if k == " ": # skip space between words
            continue
        result = k+"\t"+str(v)+"\t"+str(v/grapheme_count*100)
        print(result)
        orthography_profile.write(result+"\n")

if __name__=="__main__":
    startTime = datetime.now()
    main(sys.argv)
    print()
    print("execution time:")
    print(datetime.now()-startTime)
