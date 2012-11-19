import sys
from qlc.corpusreader import CorpusReaderWordlist
from qlc.orthography import OrthographyParser, GraphemeParser
from qlc.matrix import WordlistStoreWithNgrams
from scipy.io import mmread, mmwrite # write sparse matrices
from scipy.sparse import csr_matrix, lil_matrix, coo_matrix

if len(sys.argv) != 2:
	print("call: python matrix.py source\n")
	print("python matrix.py huber1992\n")

source = sys.argv[1] # dictionary/wordlist source key
output_dir = source+"/"

# get data from corpus reader
cr = CorpusReaderWordlist("data/csv")          # real data
# cr = CorpusReaderWordlist("data/testcorpus") # test data

# initialize orthography parser for source
o = OrthographyParser("data/orthography_profiles/"+source+".txt")
# o = GraphemeParser()

# create a generator of corpus reader data
wordlist_iterator = ( (wordlistdata_id, concept, counterpart)
	for wordlistdata_id in cr.wordlistdata_ids_for_bibtex_key(source)
	for concept, counterpart in cr.concepts_with_counterparts_for_wordlistdata_id(wordlistdata_id)
)

"""
# print all the things!
for wordlistdata_id, concept, counterpart in wordlist_iterator:
	print(wordlistdata_id+"\t"+cr.get_language_bookname_for_wordlistdata_id(wordlistdata_id)+"\t"+concept+"\t"+counterpart)
"""

# initialize matrix class
w = WordlistStoreWithNgrams(wordlist_iterator, o, "graphemes", 1) # pass ortho parser and ngram length

# Create parsed and language-specific word matrices and 
# convert to sparse matrix (csr format best for matrix multiplication)

WL = w.non_unique_words_languages_counts_matrix()
WL_sparse = csr_matrix(WL)
mmwrite(output_dir+source+"_WL.mtx", WL_sparse)

WM = w.non_unique_words_concepts_counts_matrix()
WM_sparse = csr_matrix(WM)
mmwrite(output_dir+source+"_WM.mtx", WM_sparse)

WG = w.non_unique_words_graphemes_counts_matrix()
mmwrite(output_dir+source+"_WG.mtx", WG)

GP = w.get_gp_matrix()
GP_sparse = csr_matrix(GP) 
mmwrite(output_dir+source+"_GP.mtx", GP_sparse)

# write headers (non-unique) :: can run all at once
w.write_wordlistid_languagename(source, cr) # write wordlistid \t language name
w.write_header(w.non_unique_parsed_words, source, "_words_header.txt")
w.write_header(w.concepts, source, "_meanings_header.txt")

# this creates a ngrams header without delimiter
# w.write_header(w.non_unique_ngrams, source, "_ngrams_header.txt")
w.write_header_ngrams(w.non_unique_ngrams, source, "_ngrams_header.txt")

# makes phoneme header, i.e. all unique phonemes independent of language
w.make_ngram_header(source)

# print the word and ngrams/ngrams-indices
w.write_words_ngrams_strings(source)    
w.write_words_ngrams_indices(source)
