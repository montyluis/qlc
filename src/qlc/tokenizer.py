# -*- coding: utf-8 -*-

import os
import sys
import unicodedata
from qlc.corpusreader import CorpusReaderWordlist
from qlc.orthography import OrthographyParser
from configparser import SafeConfigParser
from configparser import ConfigParser

# class tokenzier:
# read in config file
# do data manipulation / storage
# returns various data types: list of concepts tokenized...
# use to produce various things, like initial orthography profile


# NOTE: if you have more than one class at the same level in Python; both are fired if you create an object in main.
"""
class SpreadsheetParser:
    input_file = open("data/dogon/heath2012_spreadsheet_format.csv", "r")
    output_file = open("data/dogon/qlc_output_heath2012.tsv", "w")
    header = input_file.readline()
    header = header.strip()
    languages = header.split("\t")

    # print("COUNTERPART"+"\t"+"CONCEPT"+"\t"+"LANGUAGE_BOOKNAME")
    # actually: concept, counterpart, language
    
    # this is essentially Mattis's format
    for line in input_file:
        line = line.strip()
        line = unicodedata.normalize("NFD", line) # skip the END
        tokens = line.split("\t")
        concept = tokens[0].strip()
        for i in range(1, len(tokens)-1):
            counterpart = tokens[i].strip()
            if counterpart == "":
                counterpart = "NONE"
            result = counterpart+"\t"+concept+"\t"+languages[i].strip()
            # result = concept+"\t"+tokens[i].strip()+"\t"+languages[i].strip()
            # print(result)
            output_file.write(result+"\n")
    input_file.close()
    output_file.close()
    """

class Tokenizer:

    """ takes as input a file with the QLC format:
    counterpart \t concept \t language

    and does things like 

    - tokenizes the file into LINGPY format
    - tokenizes the data into ortographically parsed QLC format
    - locates unicorns

    """

    def __init__(self):
        # deal with configuration file
        # configparser.read(default.cfg)
        cfg = SafeConfigParser()
        cfg.read("default.cfg")

        data = cfg.get("Paths", "data")
        orthography_profile = cfg.get("Paths", "orthography_profile")

        # set variables, e.g. source, orthography parser, etc.
        self.data = open(data, "r")

        self.o = OrthographyParser(orthography_profile)        
        self._languages = []
        self._concepts = []
        self._counterparts = []
        self._wordlist_iterator = self._process_input(self.data)

        # print(type(self.iterator))
        # print(len(self.counterparts))
        # words = self.get_qlc_tokenized_words()

        """
        count = 0
        for line in words:
            if line != "":
                print(line)
                count += 1
        print(count)
        """

        """
        self.cr = CorpusReaderWordlist("data/csv")
        self.wordlist_iterator = ( (wordlistdata_id, concept, counterpart)
            for wordlistdata_id in self.cr.wordlistdata_ids_for_bibtex_key(source)
            for concept, counterpart in self.cr.concepts_with_counterparts_for_wordlistdata_id(wordlistdata_id)
        )
        """

    def _process_input(self, file):
        lines = []
        for line in file:
            line = line.strip()
            line = line.replace("  ", " ")
            concept, counterpart, language = line.split("\t")
            result = (concept, counterpart, language)
            lines.append(result)

            self._languages.append(language) # pointless
            self._concepts.append(concept) # pointless
            self._counterparts.append(counterpart)
        return ((concept, counterpart, language) for concept, counterpart, language in lines)


    def get_qlc_tokenized_words(self):
        unparasables = open("unparsables.txt", "w")
        tokenized_words = []
        for counterpart, concept, language in self._wordlist_iterator:
            counterpart = unicodedata.normalize("NFD", counterpart)
            grapheme_parsed_counterpart_tuple = self.o.parse_string_to_graphemes_string(counterpart)
            if grapheme_parsed_counterpart_tuple[0] == False:
                unparsables.write(grapheme_parsed_counterpart_tuple[1])
                continue
        
            grapheme_parse = grapheme_parsed_counterpart_tuple[1]
            tokenized_words.append(grapheme_parse)
        return tokenized_words

    def get_ipa_tokenized_words(self):
        tokenized_words = []
        words = get_list_qlc_tokenized_words()
        for word in words:
            grapheme_parsed_counterpart_tuple = self.o.parse_string_to_graphemes_string(counterpart)
            
    def lingpy_output(self):
        # given some data set from the corpusreader, output a lingpy format
        print("# LANGUAGE"+"\t"+"CONCEPT"+"\t"+"COUNTERPART"+"\t"+"ORTHO_PARSE")
        for counterpart, concept, language in self._wordlist_iterator:
            # skip for Mattis
            if counterpart == "?" or counterpart == "NONE":
                continue

            grapheme_parsed_counterpart_tuple = self.o.parse_string_to_graphemes_string(counterpart)
            if grapheme_parsed_counterpart_tuple[0] == False:
                continue

            ortho_parse = grapheme_parsed_counterpart_tuple[1]
            print(language+"\t"+concept+"\t"+counterpart+"\t"+grapheme_parsed_counterpart_tuple[1])

    def matrix_output(self):
        # produce Jelena style output format with matrix
        pass

    def qlc_output_format(self):
        # produce counterpart \t concept \t language QLC output format
        print("COUNTERPART"+"\t"+"ORTHO_PARSE"+"\t"+"CONCEPT"+"\t"+"LANGUAGE")
        for counterpart, concept, language in self._wordlist_iterator:
            if counterpart == "?" or counterpart == "NONE":
                print(counterpart+"\t"+counterpart+"\t"+concept+"\t"+language)                
                continue
            grapheme_parsed_counterpart_tuple = self.o.parse_string_to_graphemes_string(counterpart)
            
            # skip shit that doesn't parse
            if grapheme_parsed_counterpart_tuple[0] == False:
                continue

            ortho_parse = grapheme_parsed_counterpart_tuple[1]
            print(counterpart+"\t"+ortho_parse+"\t"+concept+"\t"+language)

if __name__=="__main__":
    from qlc.tokenizer import Tokenizer
    from qlc import ngram
    t = Tokenizer()
    t.qlc_output_format()
#    words = t.get_qlc_tokenized_words()
#    ngram.unigram_model(words)
