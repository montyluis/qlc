# -*- coding: utf-8 -*-

import sys
import regex 
import qlc.orthography
import unicodedata

"""
Script to check if the orthography profile has errors in it.

Note: you need to install regex library for this script to work.

Usage: python unique_graphems.py /path/to/orthography_profile /path/to/heads_file

If orthography file has duplicates, error will trigger.

"""

def main(argv):
    if len(argv) < 3:
        print()
        print("call: python check_orthography_profile.py /path/to/orthography_profile /path/to/heads_file")
        print()
        exit(1)


    grapheme_pattern = regex.compile("\X", regex.UNICODE) # Unicode grapheme regular expression pattern
    orthography_hash = {} # hash of graphemes in orthography profile
    headword_unicode_graphemes_hash = {} # hash of Unicode graphemes from head words
    head_words = [] # list of words from input file; typically headwords

    # load orthography hash
    orthography_profile = open(sys.argv[1], "r")
    for line in orthography_profile:
        line = line.strip()
        if line.startswith("#") or line == "":
            continue
        line = unicodedata.normalize("NFD", line)
        tokens = line.split(",")
        grapheme = tokens[0]
    
        if not grapheme in orthography_hash:
            orthography_hash[grapheme] = 1
        else:
            orthography_hash[grapheme] += 1
            print("** You have duplicates in your orthography profile - please fix them and rerun **")
            sys.exit()
    orthography_profile.close()


    # create a list containing the headwords from the heads file
    heads_file = open(sys.argv[2], "r")
    line_count = 1
    for line in heads_file:
        line = line.strip()
        if line.__contains__("COUNTERPART"): # skip possible header
            continue
        line_count += 1
        line = unicodedata.normalize("NFD", line)
        tokens = line.split("\t")
        head = tokens[0]
        head_words.append(head)
    heads_file.close()

    # load hash of unique graphemes from the dictionary heads file and get a list of headwords
    for head_word in head_words:
        graphemes = grapheme_pattern.findall(head_word)
        for grapheme in graphemes:
            grapheme = grapheme.strip()
            if grapheme == " " or grapheme == "":
                continue
            if not grapheme in headword_unicode_graphemes_hash:
                headword_unicode_graphemes_hash[grapheme] = 1
            else:
                headword_unicode_graphemes_hash[grapheme] += 1

    # print the orthography profile hash
    print("# Hash set from orthography profile (output format: grapheme+tab+count)")
    for k, v in orthography_hash.items():
        print(k+"\t"+str(v))
    print("# Total number of graphemes in your orthography profile:", len(orthography_hash))
    print()

    # print the heads unique unicode graphemes hash
    print("# Hash set of unique Unicode graphemes from headwords file (pre-orthography profile parsing):")
    for k, v in headword_unicode_graphemes_hash.items():
        print(k+"\t"+str(v))
    print("# Number of unique Unicode graphemes in headwords file:", len(headword_unicode_graphemes_hash))
    print()

    """
    # compare hashes of Unicode regex \X graphemes vs headwords
    print "**Unicode** graphemes via regex '\X' match in head words and NOT in orthograhy profile file:"
    c1 = 0
    for k, v in headword_hash.iteritems():
        if not orthography_hash.has_key(k):
            print "ERROR:", k.encode("utf-8")
            c1 += 1
    if not c1 == 0:
        print "total:", c1
        print
        """

    # check orthography profile contents against headwords
    print("# Checking if orthography profile contents are not in any headwords (does not catch elements in headwords that are missing in the orthography profile)...")
    missing_orthography_contents = []
    for k, v in orthography_hash.items():
        flag = False
        for line in head_words:
            if line.__contains__(k):
                flag = True
        if flag == False:
            missing_orthography_contents.append(k)

    if len(missing_orthography_contents) > 0:
        for item in missing_orthography_contents:
            print("# ** NOT IN HEADWORDS:", item)
        print("# Total:", len(missing_orthography_contents))
    else:
        print("# Yay! All orthography profile graphemes are present in the data!")
    print()


    invalid_parses = []

    # check headword graphemes against orthography profile contents
    orthography_profile_location = sys.argv[1]
    o = qlc.orthography.OrthographyParser(orthography_profile_location)
    ortho_parsed_graphemes = {}
    comparison_hash = {}
    incorrect_forms = []
    ln = 0
    for head in head_words:
        ln += 1
        orthography_parse_tuple = o.parse_string_to_graphemes_string(head)
        if orthography_parse_tuple[0] == False:
            invalid_parses.append(orthography_parse_tuple[1])
        orthography_parse = orthography_parse_tuple[1]
        parsed_graphemes = orthography_parse.split()
        for g in parsed_graphemes:
            if not g in ortho_parsed_graphemes:
                ortho_parsed_graphemes[g] = 1
            else:
                ortho_parsed_graphemes[g] += 1
            if not g in orthography_hash:
                comparison_hash[g] = 1
                incorrect_forms.append((ln, g, head, orthography_parse, parsed_graphemes))

    print()
    print("# Checking headword graphemes against orthography profile contents:")
    if len(comparison_hash) > 1:
        for k, v in comparison_hash.items():
            print("# ** Not in orthography profile: ", k)
        print()
        print("# Printing records that contain elements not in orthography profile...\n")
        print("Line #", "\t", "Missing", "\t", "Headword", "\t", "Orthography parse", "\t", "Orthography parse split")
        for i in incorrect_forms:
            if i[1] != "#" and i[1] != "-":
                print(i[0], "\t", i[1], "\t", i[2], "\t", i[3], "\t", i[4])

        print()
        print("# Unique graphemes and counts from orthography profile:")
        for k, v in ortho_parsed_graphemes.items():
            print(k, "\t", v)
        print()

    else:
        print("# All headword graphemes are in the orthography profile")
        print()

    print("Words that did not parse:")
    for invalid_parse in invalid_parses:
        print(invalid_parse)
    print()


    
    # print headwords and their parses
    orthography_profile_location = sys.argv[1]
    o = qlc.orthography.OrthographyParser(orthography_profile_location)
    ln = 0
    print()
    print("print headwords and their parses (if parsable; if not print just the original word/phrase/etc.):")
    print()
    print("line #"+"\t"+"original"+"\t"+"parsed form (if it parses)")
    for head in head_words:
        ln += 1
        orthography_parse_tuple = o.parse_string_to_graphemes_string(head)
        if orthography_parse_tuple[0] == True:
            orthography_parse = orthography_parse_tuple[1]
            parsed_graphemes = orthography_parse.split()
            print(str(ln)+"\t"+head+"\t"+orthography_parse)
        else:
            print(str(ln)+"\t"+head+"\t"+"")

if __name__=="__main__":
    main(sys.argv)
