# -*- coding: utf-8 -*-

import sys
import unicodedata
import qlc.orthography

"""
Example script that uses the orthography parser to parse orthographic strings into strings of graphemes given an orthography profile.

"""

def printResults(input, output, write_to_stderr):
    (success, result) = output
    if not success and write_to_stderr:
        sys.stderr.write(input + "\t" + result + "\n")
    else:
        print(result)


def main(argv):
    if len(argv) < 4:
        print("\ncall: python orthography_parser.py orthography_profile.txt data.txt transform_flag")
        print()
        print("transform_flag == the column of the orthography_profile that should be used, e.g.:")
        print()
        print("python orthography_parser.py ../data/orthography_profiles/thiesen1998.txt ../data/heads_with_translations/heads_with_translations_thiesen1998_25_339.txt 1")
        print()

        exit(1)

    # create a list containing the headwords from the heads file
    heads_file = open(sys.argv[2], "r", encoding="utf-8")
    head_words = []

    # indicate which column of the ortography profile
    transform_flag = int(sys.argv[3])

    # put the counterparts into a list to do orthographic parse
    for line in heads_file:
        line = line.strip()
        line = unicodedata.normalize("NFD", line)
        if line.__contains__("\t"):
            tokens = line.split("\t")
            head = tokens[0]
            head_words.append(head)
        else:
            head_words.append(line)
    heads_file.close()


    # parse head words with the orthography profile and print results
    orthography_profile = sys.argv[1]

    o = qlc.orthography.OrthographyParser(orthography_profile)
    for head in head_words:
        if transform_flag == 1:
            # For easy diffing, pass this False instead of true, to write everything to
            # stdout instead of redirecting the unparseables to stderr.
            printResults(head, o.parse_string_to_graphemes_string(head), True)
            # For diffing against the old version, use this one:
            # printResults(head, o.parse_string_to_graphemes_string_DEPRECATED(head), False)
        elif transform_flag == 2:
            printResults(head, o.parse_string_to_ipa_string(head))
        else:
            raise Exception("Invalid column number!")

if __name__=="__main__":
    main(sys.argv)
       
# call:
# python ex_orthography_parser.py data/orthography_profiles/thiesen1998.txt data/wordlists/heads_with_translations_thiesen1998_25_339.txt 1 > newnew 2> stderr       



