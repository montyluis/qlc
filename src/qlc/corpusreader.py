# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2011, 2012, Quantitative Language Comparison Team
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file COPYING.txt, distributed with this software.
#-----------------------------------------------------------------------------

"""
Corpus Reader for data of the project Quantitative Language Comparison.
"""

#-----------------------------------------------------------------------------
# Imports
#-----------------------------------------------------------------------------

import sys, os.path
import codecs, re, collections, fileinput, shutil


#-----------------------------------------------------------------------------
# Globals
#-----------------------------------------------------------------------------

_component_table_columns = dict(name=0, description=1)

_book_table_columns = dict(title=0, author=1, year=2, isbn=3, bibtex_key=4,
                           columns=5, pages=6, origfilepath=7, type=8,
                           is_ready=9, has_changed=10)

_dictdata_table_columns = dict(startpage=0, endpage=1, startletters=2,
                               book_id=3, component_id=4)

_language_iso_table_columns = dict(name=0, langcode=1, description=2, url=3)

_language_bookname_table_columns = dict(name=0)

_language_src_table_columns = dict(dictdata_id=0, language_iso_id=1,
                                   language_bookname_id=2)

_language_tgt_table_columns = dict(dictdata_id=0, language_iso_id=1,
                                   language_bookname_id=2)

_entry_table_columns = dict(head=0, fullentry=1, is_subentry=2,
                            is_subentry_of_entry_id=3, dictdata_id=4, book_id=5,
                            startpage=6, endpage=7, startcolumn=8,
                            endcolumn=9, pos_on_page=10,
                            has_manual_annotations=11, volume=12)

_annotation_table_columns = dict(entry_id=0, annotationtype_id=1, start=2,
                                 end=3, value=4, string=5)

_wordlistentry_table_columns = dict(fullentry=0, startpage=1, endpage=2,
                                    startcolumn=3, endcloumn=4, pos_on_page=5,
                                    concept_id=6, wordlistdata_id=7,
                                    has_manual_annotations=8, volume=9)

_wordlistdata_table_columns = dict(startpage=0, endpage=1,
                                   language_bookname_id=2, language_iso_id=3,
                                   book_id=4, component_id=5)

_wordlistconcept_table_columns = dict(concept=0)

_wordlistannotation_table_columns = dict(entry_id=0, annotationtype_id=1,
                                         start=2, end=3, value=4, string=5)

#-----------------------------------------------------------------------------
# Classes
#-----------------------------------------------------------------------------

class CorpusReaderDict(object):
    """
    The corpus reader class for dictionary data. API was designed to allow
    easy access to the data of the dictionaries, all methodes return data
    in Python data structures. Think of it as DB-less queries that return
    Key-Value-Stores.
    """
    
    __slots__ = ("__datapath", "__components", "__books", "__languages_iso",
                 "__languages_src", "__languages_tgt", "__dictdata",
                 "__entries", "__annotations", "__entry_annotations_cache",
                 "dictdata_string_ids" )
    
    def __init__(self, datapath):
        """
        Constructor of CorpusReaderDict class.
        
        Parameters
        ----------
        datapath : str
            The path to the dictionary data files (*.csv) in the file system.
        
        Returns
        -------
        Nothing
        """
        
        self.__datapath = datapath
        self.__components = {}
        self.__books = {}
        self.__languages_iso = {}
        self.__languages_src = {}
        self.__languages_tgt = {}
        self.__dictdata = {}
        self.__entries = {}
        self.__annotations = {}
        self.__entry_annotations_cache = {}
        self.dictdata_string_ids = {}
        
        re_quotes = re.compile('""')

        # read component table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "component.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__components[data.pop(0)] = data

        # read book table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "book.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__books[data.pop(0)] = data

        # read dictdata table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "dictdata.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__dictdata[data.pop(0)] = data
            
        # read entry table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "entry.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            data_stripped = []
            for d in data:
                if len(d) > 0:
                    if d[0] == '"' and d[-1] == '"':
                        d = re_quotes.sub('"', d[1:-1])
                data_stripped.append(d)
            self.__entry_annotations_cache[data_stripped[0]] =\
                collections.defaultdict(set)
            if len(data_stripped) < 7:
                print(data_stripped)
                print(data_stripped[0])
            self.__entries[data_stripped.pop(0)] = data_stripped

        # read annotation table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "annotation.csv"),
                           "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            data_stripped = []
            for d in data:
                if len(d) > 0:
                    if d[0] == '"' and d[-1] == '"':
                        d = re_quotes.sub('"', d[1:-1])
                data_stripped.append(d)
            id = data_stripped.pop(0)
            entry_id = data_stripped[_annotation_table_columns['entry_id']]
            self.__entry_annotations_cache[entry_id][
                # key is the annotation value: "head", "translation", ...
                data_stripped[_annotation_table_columns['value']]
                # value is a list of annotation strings
                ].add(data_stripped[_annotation_table_columns['string']])
            self.__annotations[id] = data_stripped
            
        # read language iso table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "language_iso.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__languages_iso[data.pop(0)] = data

        # read src language table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "language_src.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__languages_src[data.pop(0)] = data

        # read tgt language table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "language_tgt.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__languages_tgt[data.pop(0)] = data

        self.__init_dictdata_string_ids()


    def __init_dictdata_string_ids(self):
        """
        Initializer for Dictdata identification strings. Dictdata are parts of
        books that contain dictionary data (vs. Nondictdata). The string IDs
        are equal to the ID within URLs of the QuantHistLing website, i.e.
        something like "thiesen1998_10_244". The general structure of the ID is
        "key_startpage_endpage". The strings are saved into a private dict,
        mapping from the numerical ID to the string ID, to allow an easy
        lookup. This method is called by the constructor of the class and
        should not be called by the user.
        
        Parameters
        ----------
        None
            
        Returns
        -------
        Nothing
        """
        for dictdata_id in self.__dictdata:
            book_id = self.__dictdata[dictdata_id][
                      _dictdata_table_columns['book_id']]
            bibtex_key = self.__books[book_id][
                         _book_table_columns['bibtex_key']]
            self.dictdata_string_ids[dictdata_id] = "%s_%s_%s" % \
                (bibtex_key, self.__dictdata[dictdata_id][
                _dictdata_table_columns['startpage']],
                 self.__dictdata[dictdata_id][
                    _dictdata_table_columns['endpage']])

    def dictdata_string_id_for_dictata_id(self, dictdata_id):
        """
        Return the string ID to a given numerical ID of a Dictdata entry.
        Dictdata are parts of books that contain dictionary data
        (vs. Nondictdata). The string IDs are equal to the ID within URLs of the
        QuantHistLing website, i.e. something like "thiesen1998_10_244". The
        general structure of the ID is "key_startpage_endpage".
        
        Parameters
        ----------
        dictdata_id : str
            Numerical ID of the Dictdata entry.
        
        Returns
        -------
        String containing the string ID of the given Dictdata entry.
        """
        return self.dictdata_string_ids[dictdata_id]

    def dictdata_ids_for_bibtex_key(self, param_bibtex_key):
        """Return an array of dicionary parts IDs for a given book. The book
        is identified by the so-called bibtex key, which is the string for
        the book from the URL. For example: "thiesen1998".
        
        Parameters
        ----------
        bibtex_key : str
            A string with the bibtex key.
        
        Returns
        -------
        An array containing all the dictdata IDs for the book.
        """
        ret = []
        for dictdata_id in self.__dictdata:
            book_id = self.__dictdata[dictdata_id][
                      _dictdata_table_columns['book_id']]
            if self.__books[book_id][_book_table_columns['bibtex_key']] ==\
               param_bibtex_key:
                ret.append(dictdata_id)
        return ret

    def dictdata_ids_for_component(self, component):
        """Return an array of dicionary parts IDs for a given component. The
        book is identified by the so-called bibtex key, which is the string for
        the book from the URL. For example: "thiesen1998".
        
        Parameters
        ----------
        component : str
            A string with the component's name.
        
        Returns
        -------
        An array containing all the dictdata IDs for the component.
        """
        ret = []
        for dictdata_id in self.__dictdata:
            component_id = self.__dictdata[dictdata_id][
                           _dictdata_table_columns['component_id']]
            if self.__components[component_id][
               _component_table_columns['name']] == component:
                ret.append(dictdata_id)
        return ret
    

    def src_languages_iso_for_dictdata_id(self, dictdata_id):
        """
        Returns the source language for the given dictionary part as ISO code.
        
        Parameters
        ----------
        dictdata_id : str
            The ID of the dictionary part to look up
        
        Returns
        -------
        A list of ISO codes of the source languages for that bibtex_key.
        """
        src_languages = []
        for row in self.__languages_src.values():
            if row[_language_src_table_columns['dictdata_id']] == dictdata_id:
                l_id = row[_language_src_table_columns['language_iso_id']]
                if l_id:
                    src_languages.append(self.__languages_iso[l_id][
                        _language_iso_table_columns['langcode']])
                else:
                    src_languages.append(None)
        return src_languages

    def tgt_languages_iso_for_dictdata_id(self, dictdata_id):
        """
        Returns the target language for the given dictionary part as ISO code.
        
        Parameters
        ----------
        dictdata_id : str
            The ID of the dictionary part to look up.
        
        Returns
        -------
        A list of ISO codes of the target languages for that bibtex_key
        """
        tgt_languages = []
        for row in self.__languages_tgt.values():
            if row[_language_tgt_table_columns['dictdata_id']] == dictdata_id:
                l_id = row[_language_tgt_table_columns['language_iso_id']]
                if l_id:
                    tgt_languages.append(self.__languages_iso[l_id][
                        _language_iso_table_columns['langcode']])
                else:
                    tgt_languages.append(None)
        return tgt_languages


    def entry_ids_for_dictdata_id(self, dictdata_id):
        """
        Returns all entry IDs for a given dictdata ID. A dictdata ID is the
        ID of a dictionary part of a source.
        
        Parameters
        ----------
        
        dictdata_id : str
                ID of the dictdata, as Unicode string.
                
        Returns
        -------
        
        A generator for all entry IDs in that dictionary part.
        """
        return(k for k, v in self.__entries.items()
            if v[_entry_table_columns['dictdata_id']] == dictdata_id)


    def annotations_for_entry_id_and_value(self, entry_id, value):
        """
        Returns alls annotation IDs for a given entry ID and and given
        annotation value. Annotation values are strings like "head",
        "translation", etc.
        
        Parameters
        ----------
        
        entry_id : str
                ID of the entry, as Unicode string.
        value : str
                String of the annotation value to look for, i.e. "head",
                "translation", etc.
                
        Returns
        -------
        
        A generator to all the annotatotion of the entry that match the given
        annotation value.
        """
        return(a for a in self.__entry_annotations_cache[entry_id][value])

    def data(self, dictdata_id):
        """
        A wrapper for heads_with_translations_for_dictdata_id(dictdata_id).
        """
        return self.heads_with_translations_for_dictdata_id(dictdata_id)
        
    def heads_with_translations_for_dictdata_id(self, dictdata_id):
        """
        Returns alls (head, translation) pairs for a given dictdata ID.
        
        Parameters
        ----------
        dictdata_id : str
                ID of the dictdata, as Unicode string.
         
        Returns
        -------
        
        A generator for (head, translation) tuples.
        """
        return((head, translation)\
            for entry_id in self.entry_ids_for_dictdata_id(dictdata_id)
            for head in self.annotations_for_entry_id_and_value(
                        entry_id, "head")
                for translation in self.annotations_for_entry_id_and_value(
                                   entry_id, "translation"))

    def ids_with_heads_with_translations_for_dictdata_id(self, dictdata_id):
        """
        Returns alls (entry_id, head, translation) pairs for a given dictdata ID.
        
        Parameters
        ----------
        dictdata_id : str
                ID of the dictdata, as Unicode string.
         
        Returns
        -------
        
        A generator for (entry_id, head, translation) tuples.
        """
        return((entry_id, head, translation)\
            for entry_id in self.entry_ids_for_dictdata_id(dictdata_id)
            for head in self.annotations_for_entry_id_and_value(
                        entry_id, "head")
                for translation in self.annotations_for_entry_id_and_value(
                                   entry_id, "translation"))

class CorpusReaderWordlist(object):
    """
    The corpus reader class for wordlist data. API was designed to allow
    easy access to the data of the dictionaries, all methodes return data
    in Python data structures. Think of it as DB-less queries that return
    Key-Value-Stores.
    """
    
    __slots__ = ("__datapath", "__components", "__books", "__languages_iso",
                 "__languages_bookname",
                 "__wordlistdata", "__entries", "__annotations", "__concepts",
                 "__entry_annotations_cache", "wordlistdata_string_ids" )
    
    def __init__(self, datapath):
        """
        Constructor of CorpusReaderWordlist class.
        
        Parameters
        ----------
        datapath : string
            The path to the dictionary data files (*.csv) in the file system.
        
        Returns
        -------
        Nothing
        """
        
        self.__datapath = datapath
        self.__components = {}
        self.__books = {}
        self.__languages_iso = {}
        self.__languages_bookname = {}
        self.__wordlistdata = {}
        self.__entries = {}
        self.__annotations = {}
        self.__concepts = {}
        self.__entry_annotations_cache = {}
        self.wordlistdata_string_ids = {}

        # read component table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "component.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__components[data.pop(0)] = data

        # read book table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "book.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__books[data.pop(0)] = data

        # read worlistdata table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "wordlistdata.csv"),
                           "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            if len(data) < 7:
                print(data)
            self.__wordlistdata[data.pop(0)] = data

        # read wordlist entry table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "wordlistentry.csv"),
                           "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__entry_annotations_cache[data[0]] = collections.defaultdict(set)
            self.__entries[data.pop(0)] = data

        # read wordlist annotation table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "wordlistannotation.csv"),
                           "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            id = data.pop(0)
            entry_id = data[_wordlistannotation_table_columns['entry_id']]
            self.__entry_annotations_cache[entry_id][
                # key is the annotation value: "head", "translation", ...
                data[_wordlistannotation_table_columns['value']]
                # value is a list of annotation strings
                ].add(data[_wordlistannotation_table_columns['string']])
            self.__annotations[id] = data
            
        # read language iso table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "language_iso.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__languages_iso[data.pop(0)] = data

         # read language bookname table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "language_bookname.csv"), "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__languages_bookname[data.pop(0)] = data
            
        # read concept table
        is_first_line = True
        file = codecs.open(os.path.join(datapath, "wordlistconcept.csv"),
                           "r", "utf-8")
        for line in file:
            if is_first_line:
                is_first_line = False
                continue
            line = line.rstrip("\r\n")
            data = line.split("\t")
            self.__concepts[data.pop(0)] = data

        self.__init_wordlistdata_string_ids()

    def __init_wordlistdata_string_ids(self):
        """
        Initializer for Worlistdata identification strings. Wordlistdata are
        parts of books that contain wordlist data (vs. Nondictdata and
        Dictdata). The string IDs are equal to the ID within URLs of the
        QuantHistLing website, i.e. something like "huber1992_10_392". The
        strings are saved into a private dict, mapping from the numerical ID to
        the string ID, to allow an easy lookup. This method is called by the
        constructor of the class and should not be called by the user.
        
        Parameters
        ----------
        None
            
        Returns
        -------
        Nothing
        """
        for wordlistdata_id in self.__wordlistdata:
            book_id = self.__wordlistdata[wordlistdata_id][
                      _wordlistdata_table_columns['book_id']]
            bibtex_key = self.__books[book_id][
                         _book_table_columns['bibtex_key']]
            self.wordlistdata_string_ids[wordlistdata_id] = "%s_%s_%s" % (
                bibtex_key,
                self.__wordlistdata[wordlistdata_id][
                    _wordlistdata_table_columns['startpage']],
                self.__wordlistdata[wordlistdata_id][
                    _wordlistdata_table_columns['endpage']]
                )
        
    def wordlistdata_ids_for_bibtex_key(self, bibtex_key):
        """Return an array of wordlist parts IDs for a given book. The book
        is identified by the so-called bibtex key, which is the string for
        the book from the URL. For example: "huber1992".
        
        Parameters
        ----------
        bibtex_key : string
            A string with the bibtex key.

        Returns
        -------
        An iterator over all the wordlistdata IDs for the book.
        """
        ret = []
        for wordlistdata_id in self.__wordlistdata:
            book_id = self.__wordlistdata[wordlistdata_id][
                      _wordlistdata_table_columns['book_id']]
            if self.__books[book_id][_book_table_columns['bibtex_key']] ==\
               bibtex_key:
                ret.append(wordlistdata_id)
        return ret

    def wordlistdata_ids_for_component(self, component):
        """Return an array of wordlist parts IDs for a given component. The
        book is identified by the so-called bibtex key, which is the string for
        the book from the URL. For example: "huber1992".
        
        Parameters
        ----------
        component : str
            A string with the component's name.
        
        Returns
        -------
        An array containing all the wordlistdata IDs for the component.
        """
        ret = []
        for wordlistdata_id in self.__wordlistdata:
            component_id = self.__wordlistdata[wordlistdata_id][
                           _wordlistdata_table_columns['component_id']]
            
            if component_id and self.__components[component_id][
               _component_table_columns['name']] == component:
                ret.append(wordlistdata_id)
        return ret

    def get_language_bookname_for_wordlistdata_id(self, wordlistdata_id):
        """Returns the language string that is used in the book for a given
        Wordlistdata ID.
        
        Parameters
        ----------
        wordlistdata_id : int
            ID of the wordlistdata part of a book
        
        Returns
        -------
        A string of the language name in the book
        """
        language_id = self.__wordlistdata[wordlistdata_id][
                      _wordlistdata_table_columns['language_bookname_id']]
        if language_id:
            return self.__languages_bookname[language_id][
                   _language_bookname_table_columns['name']]
        else:
            return ''

    def get_language_code_for_wordlistdata_id(self, wordlistdata_id):
        """Returns the language code (ISO ) that was assigned to a source for
        a given wordlistdata ID.
        
        Parameters
        ----------
        wordlistdata_id : int
                ID of the wordlistdata part of a book
        
        Returns
        -------
        A string of the language code of the wordlist data
        """
        language_id = self.__wordlistdata[wordlistdata_id][
                      _wordlistdata_table_columns['language_iso_id']]
        if language_id:
            return self.__languages_iso[language_id][
                   _language_iso_table_columns['langcode']]
        else:
            return ''


    def entry_ids_for_wordlistdata_id(self, wordlistdata_id):
        """
        Returns all entry IDs for a given wordlistdata ID. A wordlistdata ID is
        the ID of a wordlist part of a source.
        
        Parameters
        ----------
        
        wordlistdata_id : str
                ID of the dictdata, as Unicode string.
                
        Returns
        -------
        
        A generator for all entry IDs in that dictionary part.
        """
        return(k for k, v in self.__entries.items()
            if v[_wordlistentry_table_columns['wordlistdata_id']] ==\
               wordlistdata_id)
    
        
    def concept_for_entry_id(self, entry_id):
        return self.__concepts[
            self.__entries[entry_id][
                _wordlistentry_table_columns['concept_id']
                ]][_wordlistconcept_table_columns['concept']]
        
        
    def annotations_for_entry_id_and_value(self, entry_id, value):
        """
        Returns alls annotation IDs for a given entry ID and and given
        annotation value. Annotation values are strings like "head",
        "translation", etc.
        
        Parameters
        ----------
        
        entry_id : str
                ID of the entry, as string.
        value : str
                String of the annotation value to look for, i.e. "head",
                "translation", etc.
                
        Returns
        -------
        
        A generator to all the annotatotion of the entry that match the given
        annotation value.
        """
        return(a for a in self.__entry_annotations_cache[entry_id][value])
    
    
    def counterparts_for_wordlistdata_id(self, wordlistdata_id):
        return(counterpart
            for entry_id in self.entry_ids_for_wordlistdata_id(wordlistdata_id)
                for counterpart in self.annotations_for_entry_id_and_value(
                                   entry_id, "counterpart"))
        

    def concepts_for_wordlistdata_id(self, wordlistdata_id):
        return(self.concept_for_entry_id(entry_id)
            for entry_id in self.entry_ids_for_wordlistdata_id(wordlistdata_id))

    def data(self, dictdata_id):
        """
        A wrapper for concepts_with_counterparts_for_wordlistdata_id(dictdata_id).
        """
        return self.concepts_with_counterparts_for_wordlistdata_id(dictdata_id)
        
    def concepts_with_counterparts_for_wordlistdata_id(self, wordlistdata_id):
        """Returns all pairs of concepts and counterparts for a given
        wordlistdata ID.
        
        Parameters
        ----------
        
        wordlistdata_id : str
                ID of the wordlistdata, as string.
                
        Returns
        -------
        A generator for all (concept, counterpart) tuples in the wordlist part
        of the source.
        """
        return((self.concept_for_entry_id(entry_id), counterpart)
            for entry_id in self.entry_ids_for_wordlistdata_id(wordlistdata_id)
                for counterpart in self.annotations_for_entry_id_and_value(
                                   entry_id, "counterpart"))

    def ids_with_concepts_with_counterparts_for_wordlistdata_id(self, wordlistdata_id):
        """Returns all triples of entry_ids concepts and counterparts for a
        given wordlistdata ID.
        
        Parameters
        ----------
        
        wordlistdata_id : str
                ID of the wordlistdata, as string.
                
        Returns
        -------
        A generator for all (entry_id, concept, counterpart) tuples in the
        wordlist part of the source.
        """
        return((self.concept_for_entry_id(entry_id), counterpart)
            for entry_id in self.entry_ids_for_wordlistdata_id(wordlistdata_id)
                for counterpart in self.annotations_for_entry_id_and_value(
                                   entry_id, "counterpart"))

def export_swadesh_entries(input_path, output_path=None):

    print("Input: {0}".format(input_path))
    print("Ouput: {0}".format(output_path))

    cr = CorpusReaderDict(input_path)
    print("Data loaded")

    files = [ "book.csv",
          "component.csv",
          "corpusversion.csv",
          "dictdata.csv",
          "language_iso.csv",
          "language_bookname.csv",
          "language_src.csv",
          "language_tgt.csv",
          "nondictdata.csv",
          "wordlistdata.csv",
          "wordlistconcept.csv"
        ]
    
    for f in files:
        shutil.copyfile(os.path.join(
            input_path, f), os.path.join(output_path, f))
    
    from nltk.stem.snowball import SpanishStemmer
    stemmer = SpanishStemmer()
    import qlc.utils

    #get stopwords
    stopwords = qlc.utils.stopwords_from_file(os.path.join(os.path.dirname(
        os.path.realpath(
            __file__)), "data", "stopwords", "spa.txt"))

    # load swadesh list
    swadesh_file = codecs.open(os.path.join(os.path.dirname(
        os.path.realpath(
            __file__)), "data", "swadesh", "spa.txt"), "r", "utf-8")

    swadesh_entries = []
    for line in swadesh_file:
        line = line.strip()
        for e in line.split(","):
            stem = stemmer.stem(e)
            swadesh_entries.append(stem)

    # find all entries that contain one of the swadesh words
    # save entry ids to list
    entry_ids = []

    dictdata_ids = cr.dictdata_string_ids
    for dictdata_id in dictdata_ids:
        src_language_iso = cr.src_languages_iso_for_dictdata_id(dictdata_id)
        tgt_language_iso = cr.tgt_languages_iso_for_dictdata_id(dictdata_id)
        # is there some spanish?
        if (src_language_iso != ['spa']) and (tgt_language_iso != ['spa']):
            continue

        for entry_id, head, translation in \
                cr.ids_with_heads_with_translations_for_dictdata_id(
                    dictdata_id):
            if src_language_iso == [ 'spa' ]:
                (head, translation) = (translation, head)

            translation = re.sub(" ?\([^)]\)", "", translation)
            if translation in stopwords:
                entry_ids.append(entry_id)
            else:
                translation = qlc.utils.remove_stopwords(translation, stopwords)
                phrase_stems = qlc.utils.stem_phrase(translation, stemmer, True)
                for stem in phrase_stems:
                    if stem in swadesh_entries:
                        entry_ids.append(entry_id)

    #print(len(entry_ids))
    #return

    input_entry_csv = os.path.join(input_path, "entry.csv")
    output_entry_csv = os.path.join(output_path, "entry.csv")

    input_annotation_csv = os.path.join(input_path, "annotation.csv")
    output_annotation_csv = os.path.join(output_path, "annotation.csv")

    output_annotation = codecs.open(output_annotation_csv, "w", "utf-8")

    annotation_dict = collections.defaultdict(list)

    # cache annotations for lookup
    for i, line in enumerate(fileinput.input(
            input_annotation_csv, openhook=fileinput.hook_encoded("utf-8"))):
        if i == 0:
            output_annotation.write(line)
            continue
        data = line.strip().split("\t")
        annotation_dict[
            data[_annotation_table_columns['entry_id'] + 1]].append(line)
    
    fileinput.nextfile()

    output = codecs.open(output_entry_csv, "w", "utf-8")
    
    count_entries = 0
    for i, line in enumerate(fileinput.input(
            input_entry_csv, openhook=fileinput.hook_encoded("utf-8"))):
        if i == 0:
            output.write(line)
            continue
        data = line.strip().split("\t")
        if data[0] in entry_ids:
            output.write(line)
            for annotation_line in annotation_dict[data[0]]:
                output_annotation.write(annotation_line)

    fileinput.nextfile()
    output.close()
    output_annotation.close()
    
    # Worldists
    cr = CorpusReaderWordlist(sys.argv[1])
    print("Data loaded")

    # find all entries that contain one of the swadesh words
    # save entry ids to list
    wordlistdata_ids = cr.wordlistdata_string_ids
    bibtex_keys = collections.defaultdict(list)
    for wid in wordlistdata_ids:
        wordlistdata_string = cr.wordlistdata_string_ids[wid]
        bibtex_key = wordlistdata_string.split("_")[0]
        bibtex_keys[bibtex_key].append(wid)

    wordlistentry_ids = []
    for bibtex_key in bibtex_key:
        # first collect all concepts in this book where the spanish counterpart
        # has one of the swadesh words
        concepts = []
        for wordlistentry_id in wordlistentry_ids:
            language_iso = cr.get_language_code_for_wordlistdata_id(
                wordlistdata_id)
            # is there some spanish?
            if language_iso != ['spa']:
                continue

            for entry_id, concept, counterpart in \
                    cr.ids_with_concepts_with_counterparts_for_dictdata_id(
                        dictdata_id):

                counterpart = re.sub(" ?\([^)]\)", "", counterpart)
                if counterpart in stopwords:
                    entry_ids.append(entry_id)
                else:
                    counterpart = qlc.utils.remove_stopwords(
                        counterpart, stopwords)
                    phrase_stems = qlc.utils.stem_phrase(
                        counterpart, stemmer, True)
                    for stem in phrase_stems:
                        if stem in swadesh_entries:
                            concepts.append(concept)

        # now collect the entry ids for those concepts
        for wordlistentry_id in wordlistentry_ids:

            for entry_id, concept, counterpart in \
                    cr.ids_with_concepts_with_counterparts_for_dictdata_id(
                        dictdata_id):
                if concept in concepts:
                    wordlistentry_ids.append(entry_id)
    
    input_entry_csv = os.path.join(input_path, "wordlistentry.csv")
    output_entry_csv = os.path.join(output_path, "wordlistentry.csv")

    input_annotation_csv = os.path.join(input_path, "wordlistannotation.csv")
    output_annotation_csv = os.path.join(output_path, "wordlistannotation.csv")

    output_annotation = codecs.open(output_annotation_csv, "w", "utf-8")

    annotation_dict = collections.defaultdict(list)

    for i, line in enumerate(fileinput.input(input_annotation_csv, openhook=fileinput.hook_encoded("utf-8"))):
        if i == 0:
            output_annotation.write(line)
            continue
        data = line.strip().split("\t")
        annotation_dict[data[_wordlistannotation_table_columns['entry_id'] + 1]].append(line)
    
    fileinput.nextfile()

    output = codecs.open(output_entry_csv, "w", "utf-8")
    count_entries = 0
    for i, line in enumerate(fileinput.input(input_entry_csv, openhook=fileinput.hook_encoded("utf-8"))):
        if i == 0:
            output.write(line)
            continue
        data = line.strip().split("\t")
        if data[0] in entry_ids:
            output.write(line)
            for annotation_line in annotation_dict[data[0]]:
                output_annotation.write(annotation_line)

    fileinput.nextfile()
    output.close()
    output_annotation.close()    


if __name__ == "__main__":
    MAX_ENTRIES = 100
    MAX_WORDLIST_ENTRIES = 100

    if len(sys.argv) < 3:
        print("call: corpusreader.py input_path output_path")
        sys.exit(1)

    files = [ "book.csv",
              "component.csv",
              "corpusversion.csv",
              "dictdata.csv",
              "language_iso.csv",
              "language_bookname.csv",
              "language_src.csv",
              "language_tgt.csv",
              "nondictdata.csv",
              "wordlistdata.csv",
              "wordlistconcept.csv"
            ]

    for f in files:
        shutil.copyfile(os.path.join(sys.argv[1], f), os.path.join(sys.argv[2], f))
    
    cr = CorpusReaderDict(sys.argv[1])
    print("Data loaded")
    
    dictdata_ids = cr.dictdata_ids_for_bibtex_key("thiesen1998")
    
    input_entry_csv = os.path.join(sys.argv[1], "entry.csv")
    output_entry_csv = os.path.join(sys.argv[2], "entry.csv")

    input_annotation_csv = os.path.join(sys.argv[1], "annotation.csv")
    output_annotation_csv = os.path.join(sys.argv[2], "annotation.csv")
    output_annotation = open(output_annotation_csv, "w")
    annotation_dict = collections.defaultdict(list)
    for i, line in enumerate(fileinput.input(input_annotation_csv)):
        if i == 0:
            output_annotation.write(line)
        data = line.strip().split("\t")
        annotation_dict[data[_annotation_table_columns['entry_id'] + 1]].append(line)
    
    fileinput.nextfile()

    output = open(output_entry_csv, "w")
    count_entries = 0
    for i, line in enumerate(fileinput.input(input_entry_csv)):
        if i == 0:
            output.write(line)
        data = line.strip().split("\t")
        if data[_entry_table_columns['dictdata_id'] + 1] == dictdata_ids[0]:
            output.write(line)
            for annotation_line in annotation_dict[data[0]]:
            	output_annotation.write(annotation_line)
            count_entries += 1
        
        if count_entries > MAX_ENTRIES:
            break

    fileinput.nextfile()
    output.close()
    output_annotation.close()
    
    # Worldists
    cr = CorpusReaderWordlist(sys.argv[1])
    print("Data loaded")
    
    wordlistdata_ids = cr.wordlistdata_ids_for_bibtex_key("huber1992")

    input_entry_csv = os.path.join(sys.argv[1], "wordlistentry.csv")
    output_entry_csv = os.path.join(sys.argv[2], "wordlistentry.csv")

    input_annotation_csv = os.path.join(sys.argv[1], "wordlistannotation.csv")
    output_annotation_csv = os.path.join(sys.argv[2], "wordlistannotation.csv")
    output_annotation = open(output_annotation_csv, "w")
    annotation_dict = collections.defaultdict(list)
    for i, line in enumerate(fileinput.input(input_annotation_csv)):
        if i == 0:
            output_annotation.write(line)
        data = line.strip().split("\t")
        annotation_dict[data[_wordlistannotation_table_columns['entry_id'] + 1]].append(line)
    
    fileinput.nextfile()

    output = open(output_entry_csv, "w")
    count_entries = 0
    for i, line in enumerate(fileinput.input(input_entry_csv)):
        if i == 0:
            output.write(line)
        data = line.strip().split("\t")
        if data[_wordlistentry_table_columns['wordlistdata_id'] + 1] in wordlistdata_ids:
            output.write(line)
            for annotation_line in annotation_dict[data[0]]:
            	output_annotation.write(annotation_line)
            count_entries += 1

        if count_entries > MAX_WORDLIST_ENTRIES:
            break

    fileinput.nextfile()
    output.close()
    output_annotation.close()