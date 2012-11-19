# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2011, 2012, Quantitative Language Comparison Team
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file COPYING.txt, distributed with this software.
#-----------------------------------------------------------------------------

import os, types
import numpy.testing

from qlc.corpusreader import CorpusReaderDict, CorpusReaderWordlist

class testCorpusReaderDict(numpy.testing.TestCase):
    
    @classmethod 
    def setupAll(cls):
        data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data", "testcorpus")
        if not os.path.exists(data_path):
            raise(IOError("The data path {0} could not be found.".format(data_path)))
        cls.cr = CorpusReaderDict(data_path)
    
    def test_dictdata_string_ids(self):
        assert("thiesen1998_25_339" in self.cr.dictdata_string_ids.values())
        
    def test_dictdata_string_id_for_dictata_id(self):
        dictdata_id = self.cr.dictdata_ids_for_bibtex_key("thiesen1998")[0]
        assert self.cr.dictdata_string_id_for_dictata_id(dictdata_id) == "thiesen1998_25_339"

    def test_dictdata_ids_for_bibtex_key(self):
        dictdata_ids = self.cr.dictdata_ids_for_bibtex_key("thiesen1998")
        assert isinstance(dictdata_ids, list)
        assert len(dictdata_ids) == 1
        
    def test_dictdata_ids_for_component(self):
        dictdata_ids = self.cr.dictdata_ids_for_component("Witotoan")
        assert isinstance(dictdata_ids, list)
        assert len(dictdata_ids) > 0
        
    def test_src_languages_iso_for_dictdata_id(self):
        dictdata_id = self.cr.dictdata_ids_for_bibtex_key("thiesen1998")[0]
        assert self.cr.src_languages_iso_for_dictdata_id(dictdata_id) == [ "boa" ]

    def test_tgt_languages_iso_for_dictdata_id(self):
        dictdata_id = self.cr.dictdata_ids_for_bibtex_key("thiesen1998")[0]
        assert self.cr.tgt_languages_iso_for_dictdata_id(dictdata_id) == [ "spa" ]

    def test_heads_with_translations_for_dictdata_id(self):
        dictdata_id = self.cr.dictdata_ids_for_bibtex_key("thiesen1998")[0]
        generator = self.cr.heads_with_translations_for_dictdata_id(dictdata_id)
        assert type(generator) == types.GeneratorType
        (head, translation) = generator.__next__()
        print("Head: {0}, Translation {1}".format(head, translation))
        (head, translation) = generator.__next__()
        print("Head: {0}, Translation {1}".format(head, translation))

class testCorpusReaderWordlist(numpy.testing.TestCase):
    
    @classmethod 
    def setupAll(cls):
        data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "data", "testcorpus")
        if not os.path.exists(data_path):
            raise(IOError("The data path {0} could not be found.".format(data_path)))
        cls.cr = CorpusReaderWordlist(data_path)
    
    def test_dictdata_string_ids(self):
        assert("huber1992_1_367" in self.cr.wordlistdata_string_ids.values())
        
    def test_dictdata_ids_for_bibtex_key(self):
        dictdata_ids = self.cr.wordlistdata_ids_for_bibtex_key("huber1992")
        assert isinstance(dictdata_ids, list)
        assert len(dictdata_ids) > 0
        
    def test_dictdata_ids_for_component(self):
        dictdata_ids = self.cr.wordlistdata_ids_for_component("Sogeram")
        assert isinstance(dictdata_ids, list)
        assert len(dictdata_ids) > 0
        
    def test_get_language_bookname_for_wordlistdata_id(self):
        dictdata_id = self.cr.wordlistdata_ids_for_bibtex_key("huber1992")[0]
        lang = self.cr.get_language_bookname_for_wordlistdata_id(dictdata_id)
        assert isinstance(lang, str)
        assert lang != ""

    def test_get_language_code_for_wordlistdata_id(self):
        dictdata_id = self.cr.wordlistdata_ids_for_bibtex_key("huber1992")[0]
        iso = self.cr.get_language_code_for_wordlistdata_id(dictdata_id)
        assert isinstance(iso, str)
        assert iso != ""

    def test_concepts_with_counterparts_for_wordlistdata_id(self):
        dictdata_id = self.cr.wordlistdata_ids_for_bibtex_key("huber1992")[0]
        generator = self.cr.concepts_with_counterparts_for_wordlistdata_id(dictdata_id)
        assert type(generator) == types.GeneratorType
        (head, translation) = generator.__next__()
        print("Head: {0}, Translation {1}".format(head, translation))
        (head, translation) = generator.__next__()
        print("Head: {0}, Translation {1}".format(head, translation))
