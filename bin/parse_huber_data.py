# -*- coding: utf-8 -*-

import sys
import operator # python-sort-a-dictionary-by-value
import qlc.orthography
import qlc.ngram

"""
script to parse the huber data and output ngram matrices

"""

if len(sys.argv) != 4:
    print("For example, call:")
    print("python parse_huber_data.py 2 data/orthography_profiles/huber1992.txt data/concepts_with_counterparts/concepts_with_counterparts_huber1992.txt") 
    sys.exit(1)
    
# load orthography profile to parse linguistic forms from the dictionary
# load the ngram class to parse and calculate freqs of ngmrams
# load the huber database dump

o = qlc.orthography.OrthographyParser(sys.argv[2])

# start the script
file = open(sys.argv[3], "r")

file_header = file.readline()
# counterparts header looks like:
# COUNTERPART CONCEPT LANGUAGE_BOOKNAME LANGUAGE_CODE BIBTEX_KEY
concept_container = {} 
data_container = {}
ngram_container = {}
language_ngram_sums = {}
language_names = {}

for line in file:
    line = line.strip()
    columns = line.split("\t")
    counterpart = columns[0]
    concept = columns[1]
    language_bookname = columns[2]
    language_code = columns[3]
    bibtex_key = columns[4]

    if language_code == "spa" or language_code == "eng":
        continue

    # store the data
    if not language_bookname in data_container:
        data_container[language_bookname] = {}
        data_container[language_bookname][concept] = {}
    else:
        if not concept in data_container[language_bookname]:
            data_container[language_bookname][concept] = {}

    # store the concepts
    if not concept in concept_container:
        concept_container[concept] = 1
    else:
        concept_container[concept] += 1

    # store language names
    if not language_bookname in language_names:
        language_names[language_bookname] = 1
    else:
        language_names[language_bookname] += 1

    # do the orthography parsing
    orthography_parse_tuple = o.parse_string_to_ipa_string(counterpart) # set to parse to Ipa; see also o.parse() for just graphemic parse
    orthography_parse = orthography_parse_tuple[1]
    split_orthography_parse = tuple(orthography_parse.split())
    ngram_tuple = qlc.ngram.ngrams_from_graphemes(split_orthography_parse, int(sys.argv[1]))

# n.getNgramsFromList(split_orthography_parse, int(sys.argv[1])) 

    print(ngram_tuple)

    # keep track of all ngrams across all languages and their counts
    for tuple in ngram_tuple:
        print(tuple)
        if not tuple in ngram_container:
            ngram_container[tuple] = v
        else:
            ngram_container[tuple] += v

    # populate grams into hash of hash of hash
    for k, v in ngram_hash.items():
        if not k in data_container[language_bookname][concept]:
            data_container[language_bookname][concept][k] = v
        else:
            data_container[language_bookname][concept][k] += v            

    # populate language names and gram sums hash
    if not language_ngram_sums.has_key(language_bookname):
        language_ngram_sums[language_bookname] = {}
        for k, v in ngram_hash.iteritems():
            if not language_ngram_sums[language_bookname].has_key(k):
                language_ngram_sums[language_bookname][k] = v
            else:
                language_ngram_sums[language_bookname[k]] += v
    else:
        for k, v in ngram_hash.iteritems():
            if not language_ngram_sums[language_bookname].has_key(k):
                language_ngram_sums[language_bookname][k] = v
            else:
                language_ngram_sums[language_bookname][k] += v
                                    
                                    
    #print counterpart.encode("utf-8"), "\t", orthography_parse, "\t", split_orthography_parse, "\t", ngram_hash
    #print

#print ngram_container
#print len(ngram_container)

# print data container
def printDataContainer():
    for k, v in data_container.items():
        print(k, v)
    print()


# print ngrams by languages

# language_names_sorted = sorted(language_names.iteritems(), key=operator.itemgetter(1), reverse=True) 
# print language_names_sorted
def printLanguageNgrams():
    for k, v in language_ngram_sums.items():
        print(k, v)
        print()

def printSumGramsByLanguages():
    s = "ngram"
    for k, v in language_names.items():
        s += "\t"+k
    print(s)


    for ngram, count in ngram_container.items():
        row = ngram
        for lang_name, concept_count in language_names.items():
            if ngram in language_ngram_sums[lang_name]:
                sum = language_ngram_sums[lang_name][ngram]
                row += "\t"+str(sum)
            else:
                row += "\t"+"0"
        print(row.rstrip("\t"))
        

# printSumGramsByLanguages()

def printAllNgrams():    
    for k, v in ngram_container.items():
        print(k+"\t"+v)


# print everything
def printGramsByConceptsMatrix():
    s = "lang_name"+"\t"+"ngram"
    for k, v in concept_container.items():
        s += "\t"+k
    print(s.rstrip("\t"))

    # iterate through outter hash in container: language_bookname: {}
    s = ""
    for language_name, concept_hash in data_container.items():

        # iterate over all ngrams
        for ngram, ngram_count in ngram_container.items():
            s += language_name+"\t"+ngram
            # iterate through the x header of concepts
            for concept, ngram_hash in concept_container.items():

                # check to see if the language has the concept
                if concenpt in data_container[language_name]:

                    # if language has concept, check to see if that counterpart has the ngram
                    if ngram in data_container[language_name][concept]:
                        count = data_container[language_name][concept][ngram]
                        s += "\t"+str(count)
                    else:
                        s += "\t"+"0"

                else:
                    s += "\t"+"NA"
                    
            print(s.rstrip("\t"))
            s = ""

printGramsByConceptsMatrix()

"""
# checks out ok -- 366 concepts
check_hash = {}
for k, v in data_container.iteritems():
    #print k.encode("utf-8"), v
    for k, v in v.iteritems():
        print k
        if not check_hash.has_key(k):
            check_hash[k] = 1
        else:
            check_hash[k] += 1
print len(check_hash)
"""

#print len(ngram_container)
