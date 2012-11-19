# -*- coding: utf-8 -*-
#!/usr/bin/env python3
#-----------------------------------------------------------------------------
# Copyright (c) 2012, Quantitative Language Comparison Team
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file COPYING.txt, distributed with this software.
#-----------------------------------------------------------------------------

import sys
import os
import qlc.corpusreader


def main(argv):

    if len(argv) < 2:
        print("call: heads_with_translations.py data_path [output_path]")
        exit(1)

    output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "src", "qlc", "data", "swadeshcorpus")
    if len(argv) == 3:
    	output_path = argv[2]

    qlc.corpusreader.export_swadesh_entries(argv[1], output_path)

if __name__ == "__main__":
    main(sys.argv)
