import pypipegraph 


#url = 'http://code.google.com/p/pypipegraph'  #what website to download.
#target_file = 'website.html'  # where to store the downloaded website
#output_filename = 'result.tab'  # where to store the final counts


import sys
import os
import codecs
import collections
import unicodedata
import re
import glob
import pickle
import time

from qlc.corpusreader import CorpusReaderDict
from networkx import Graph
from qlc.translationgraph import write, read

re_quotes = re.compile('"')

filename_combined_graph = "combined_graph.dot"
filename_corpusreader = "corpusreader.pickle"

class NoSpanishException(Exception): pass

def escape_string(s):
    ret = re_quotes.sub('', s)
    #if not ret.startswith('"') or not ret.endswith('"'):
    #    ret = '"{0}"'.format(ret)
    return ret

#cr = None
#dictdata_ids = None
loaded_data = {}
def main(argv):
    
    if len(argv) < 3:
        print("call: translations_spanish_graph.py data_path (bibtex_key|component)")
        sys.exit(1)

    # This creates a global Pipegraph object 
    # All new jobs will automatically register with it.
    pypipegraph.new_pipegraph() 

    invariants_csv_files = []
    for file in glob.glob(os.path.join(argv[1], "*.csv")):
        invariants_csv_files.append(pypipegraph.FileTimeInvariant(file))
    
    dictdata_ids = []
    
    def load_dictdata_ids():
        cr = loaded_data["cr"]
        dictdata_ids = cr.dictdata_ids_for_bibtex_key(argv[2])
        if len(dictdata_ids) == 0:
            dictdata_ids = cr.dictdata_ids_for_component(argv[2])
            if len(dictdata_ids) == 0:
                print("did not find any dictionary data for the bibtex_key or component {0}.".format(argv[2]))
                sys.exit(1)
        return dictdata_ids
        
    def create_corpusreader():
        cr = CorpusReaderDict(argv[1])
        return cr
    
    def set_corpusreader(value):
        loaded_data["cr"] = value
        loaded_data["dictdata_ids"] = load_dictdata_ids()

    cr_loading_job = pypipegraph.CachedDataLoadingJob(filename_corpusreader, create_corpusreader, set_corpusreader)
    cr_loading_job.depends_on(invariants_csv_files)
    

    def generate_dictdata_graph_job(dictdata_id):

        cr = loaded_data["cr"]
        dictdata_string = cr.dictdata_string_id_for_dictata_id(dictdata_id)
        target_file = "{0}.dot".format(dictdata_string)

        # now, we need a function that downloads from url and stores to target_file
        def generate_dictdata_graph():
            gr = Graph()
            src_language_iso = cr.src_language_iso_for_dictdata_id(dictdata_id)
            tgt_language_iso = cr.tgt_language_iso_for_dictdata_id(dictdata_id)
            if src_language_iso != 'spa' and tgt_language_iso != 'spa':
                raise(NoSpanishException)
            
            language_iso = None
            if tgt_language_iso == 'spa':
                language_iso = src_language_iso
            else:
                language_iso = tgt_language_iso
                            
            bibtex_key = dictdata_string.split("_")[0]
    
            for head, translation in cr.heads_with_translations_for_dictdata_id(dictdata_id):
                if src_language_iso == 'spa':
                    (head, translation) = (translation, head)
                    
                head_with_source = escape_string("{0}|{1}".format(head, bibtex_key))
                translation = escape_string(translation)
                
                #translation_with_language = "{0}|{1}".format(translation, language_iso)
                
                #if head_with_source not in gr:
                gr.add_node(head_with_source, attr_dict={ "lang": language_iso, "source": bibtex_key })
                
                #if translation not in gr:
                gr.add_node(translation, attr_dict={ "lang": "spa" })
                    
                #if not gr.has_edge((head_with_source, translation)):
                gr.add_edge(head_with_source, translation)
    
            output = codecs.open(target_file, "w", "utf-8")
            output.write(write(gr))
            output.close()
        return pypipegraph.FileGeneratingJob(target_file, generate_dictdata_graph)
        
    def gen_jobs():
        cr = loaded_data["cr"]
        jobs_generate_dot = [generate_dictdata_graph_job(dictdata_id) for dictdata_id in loaded_data["dictdata_ids"]
                            if cr.src_language_iso_for_dictdata_id(dictdata_id) == "spa" or
                                cr.tgt_language_iso_for_dictdata_id(dictdata_id) == "spa"]
        for job in jobs_generate_dot:
            job.depends_on(cr_loading_job)
        def combine_graphs():
            gr = None
            for dictdata_id in loaded_data["dictdata_ids"]:
                #dictdata_string = cr.dictdata_string_id_for_dictata_id(dictdata_id)
                #target_file = "{0}.dot".format(dictdata_string)
                j = generate_dictdata_graph_job(dictdata_id)
                target_file = j.job_id
                IN = codecs.open(target_file, "r", "utf-8")
                if gr == None:
                    gr = read(IN.read())
                else:
                    gr2 = read(IN.read())
                    for node in gr2:
                        gr.add_node(node, gr2.node[node])
                    for n1, n2 in gr2.edges_iter():
                        gr.add_edge(n1, n2, gr2.edge[n1][n2])
                IN.close()
            OUT = codecs.open(filename_combined_graph, "w", "utf-8")
            OUT.write(write(gr))
            OUT.close()
    
        job_combine_graphs = pypipegraph.FileGeneratingJob(filename_combined_graph, combine_graphs)
        job_combine_graphs.depends_on(jobs_generate_dot)

    pypipegraph.JobGeneratingJob("makejobs", gen_jobs).depends_on(cr_loading_job)


    pypipegraph.run_pipegraph()
        
if __name__ == "__main__":
    main(sys.argv)
