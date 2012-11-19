# -*- coding: utf-8 -*-

import sys
import unicodedata
from qlc.corpusreader import CorpusReaderWordlist
from qlc.orthography import OrthographyParser, OrthographyRulesParser

unparsables = open("unparsables.txt", "w")

def report_unparsables(wordlistdata_id, concept, counterpart, parsed_counterpart_tuple):
    invalid_parse_string = parsed_counterpart_tuple[1]
    error = wordlistdata_id+"\t"+concept+"\t"+counterpart+"\t"+invalid_parse_string
    unparsables.write(error)


if len(sys.argv) != 2:
    print("call: python parse_counterparts.py bibtex_key_source\n")

source = sys.argv[1]

# cr = CorpusReaderWordlist("data/testcorpus")
cr = CorpusReaderWordlist("data/csv")

o = OrthographyParser("data/orthography_profiles/"+source+".txt")
rules = OrthographyRulesParser("data/orthography_profiles/"+"rules_"+source+".txt")


# create a generator of corpus reader data
wordlist_iterator = ( (wordlistdata_id, concept, counterpart)
for wordlistdata_id in cr.wordlistdata_ids_for_bibtex_key(source)
for concept, counterpart in cr.concepts_with_counterparts_for_wordlistdata_id(wordlistdata_id)
)


# print header
print("wordlist_id"+"\t"+"language_book_name"+"\t"+"concept"+"\t"+"counterpart"+"\t"+"graphemic_parse"+"\t"+"ipa_parse"+"\t"+"orthographic_rules_parse")

err_count = 0
errors = ""

# print all the things!
for wordlistdata_id, concept, counterpart in wordlist_iterator:
    # counterpart = unicodedata.normalize("NFD", counterpart)
    grapheme_parsed_counterpart_tuple = o.parse_string_to_graphemes_string(counterpart)
    phoneme_parsed_counterpart_tuple = o.parse_string_to_ipa_string(counterpart)
    
    if grapheme_parsed_counterpart_tuple[0] == False:
        report_unparsables(wordlistdata_id, concept, counterpart, grapheme_parsed_counterpart_tuple)
        continue

    if phoneme_parsed_counterpart_tuple[0] == False:
        report_unparsables(wordlistdata_id, concept, counterpart, phoneme_parsed_counterpart_tuple)
        continue
    
    grapheme_parse = grapheme_parsed_counterpart_tuple[1]
    phoneme_parse = phoneme_parsed_counterpart_tuple[1]
    rule_parsed_grapheme_parse = rules.parse_string(grapheme_parse)

    print(wordlistdata_id+"\t"+cr.get_language_bookname_for_wordlistdata_id(wordlistdata_id)+"\t"+concept+"\t"+counterpart+"\t"+grapheme_parse+"\t"+phoneme_parse+"\t"+rule_parsed_grapheme_parse)

    # check for errors in the regex rule parsing
    if not phoneme_parse == rule_parsed_grapheme_parse:
        err_count += 1
        errors += wordlistdata_id
        errors += concept
        errors += counterpart
        errors += ("pp: "+phoneme_parse+"\n")
        errors += ("gp: "+grapheme_parse+"\n")
        errors += ("rp: "+rule_parsed_grapheme_parse+"\n")

        """ # for further debugging
        for i in range(0, len(phoneme_parse)):
            print(ord(phoneme_parse[i]), "-", end=" ")
        print()

        for i in range(0, len(rule_parsed_grapheme_parse)):
            print(ord(rule_parsed_grapheme_parse[i]), "-", end=" ")
        print()
        """
errors += ("no match count: "+str(err_count))
print("no match count:", str(err_count), "(check std.err)")
sys.stderr.write(errors)
