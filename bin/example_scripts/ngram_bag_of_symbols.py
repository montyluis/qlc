# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
Documentation.

Authors:

* The Team
"""

#-----------------------------------------------------------------------------
#  Copyright (C) 2011
#  The Quantitative Language Comparison (scikits.qlc) Team
#
#  Distributed under the terms of the BSD License.
#
#  The full license is in the file COPYING.txt, distributed with this software.
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------
import sys, os, itertools, collections

import numpy
import scipy.misc

import qlc
from qlc.corpusreader import CorpusReaderWordlist
from qlc.orthography import OrthographyParser
from qlc.datatypes import WordlistStoreWithNgrams

def main(argv):

    if len(argv) < 2:
        print("call: counterparts_huber1992.py data_path")
        exit(1)

    cr = CorpusReaderWordlist(argv[1])
    o = OrthographyParser(qlc.get_data("orthography_profiles/huber1992.txt"))
    
    wordlist_iterator = ( (wordlistdata_id, concept, counterpart)
        for wordlistdata_id in cr.wordlistdata_ids_for_bibtex_key('huber1992')
        for concept, counterpart in cr.concepts_with_counterparts_for_wordlistdata_id(wordlistdata_id)
    )
    
    wordlist = WordlistStoreWithNgrams(wordlist_iterator, o)
    
    matrix_dict = dict()

    for wordlistdata_id in wordlist.languages:

        language_bookname = cr.get_language_bookname_for_wordlistdata_id(wordlistdata_id)
        #language_code = cr.get_language_code_for_wordlistdata_id(wordlistdata_id)

        if language_bookname != "bora" and language_bookname != "muinane":
            continue

        print("Creating matrix for language {0}...".format(language_bookname))
                
        matrix = numpy.zeros( (len(wordlist.concepts), len(wordlist.unique_ngrams)) )
        
        for i, concept in enumerate(wordlist.concepts):
            for j, n in enumerate(wordlist.unique_ngrams):
                if n in wordlist.counterpart_for_language_and_concept(wordlistdata_id, concept):
                    matrix[i][j] = 1
        
        matrix_dict[language_bookname] = matrix
    
    # sum up over all languages
    #languages = matrix_dict.keys()
    #matrix_languages = numpy.zeros( (len(languages), len(master_ngrams)) )
    #for i, l in enumerate(languages):
    #    matrix_languages[i] = numpy.sum(matrix_dict[l], 0)[0]
            
    #numpy.savetxt("matrix_languages.txt", matrix_languages)
    
    print('Begin comparison of two languages... Bora and Muninane!')
    print
    
    languages_tuples = [ ("bora", "muinane") ]
    
    # for each language to get a matrix of bigrams by meanings
    
    for language1, language2 in languages_tuples:
        matrix1 = matrix_dict[language1]
        matrix2 = matrix_dict[language2]
        
        n1 = wordlist.unique_ngrams.index(('e', '#'))
        n2 = wordlist.unique_ngrams.index(('o', '#'))
        
        matrix_cooccurrences = numpy.dot(numpy.transpose(matrix1), matrix2)
        
        vector1 = numpy.sum(matrix1, 0)
        vector2 = numpy.sum(matrix2, 0)
        
        print(vector1[n1])
        print(vector2[n2])
        
        print(matrix_cooccurrences[n1][n2])
        
        matrix_expectations = numpy.outer(vector1, vector2) / len(wordlist.concepts)

        print(matrix_expectations[n1][n2])

        matrix_significance = matrix_expectations + \
                              numpy.log(scipy.misc.factorial(matrix_cooccurrences)) - \
                              matrix_cooccurrences * numpy.log(matrix_expectations)
        
        numpy.savetxt("matrix_significance.txt", matrix_significance)
        
        print(matrix_significance[n1][n2])
        

    
if __name__ == "__main__":
    main(sys.argv)
