"""Knowledge base indexer.

Usage:
    parse_kb.py wiki start <start> end <end> dirpath <path_to_dir> log <path_to_log> index <index_name>
    parse_kb.py ck12 filename <path_to_file> log <path_to_log> index <index_name>
    parse_kb.py dbp filename <path_to_file> log <path_to_log> index <index_name>

Example:
    python parse_wiki.py start 1 end 50
"""

import os
import sys
import string

import re
import gzip
import logging
import pandas as pd

import nltk
from docopt import docopt, DocoptExit
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import DocType, String

ES_CONFIGURATIONS = {
    #'ES_HOSTS' : ['localhost'],
    'ES_HOSTS' : ['45.79.173.234'],
    'ES_PORTS' : [9200],
    'ES_INDEX' : None
}

elasticsearch_engine = connections.create_connection(hosts=ES_CONFIGURATIONS['ES_HOSTS'],
                                                     ports=ES_CONFIGURATIONS['ES_PORTS'])

logger = logging.getLogger('kbase-parser')
logger.setLevel(logging.DEBUG)

title_wiki = re.compile(r"\[\[([\w\s]+)\]\]+")
heading_wiki = re.compile(r"=+([\w\s]+)=+")
footer_wiki = re.compile(r'^[=\s]+(External links|See also|\w*References\w*|Further reading|Criticisms|\w*Sources\w*)[\s=]+$', re.IGNORECASE)

heading_ck12 = re.compile(r"^[ \t]*([a-zA-Z]+)[ \t]*\r", re.IGNORECASE)
footer_ck12 = re.compile(r"^Figure [\d\.]+$|^Summary$|^Explore More[ \t]*\r", re.IGNORECASE)


SENTENCE = 0

def cleaned(doc):
    if type(doc) is str:
        doc = doc.split()
    doc = map(lambda x: x.strip(' \n\t'+string.punctuation), doc)
    doc = filter(lambda x: len(x)>0, doc)
    doc = " ".join(doc)
    return doc.decode('utf-8', 'ignore').encode('ascii', 'ignore')

def index_paragraph(paragraph, paratype, keywords, filename, keyid):
    paragraph = cleaned(paragraph)
    keywords = cleaned(keywords)
    if paratype is not 'footer' and len(paragraph)>0:
        if SENTENCE:
            sid = 0
            sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')
            for sentence in sent_detector.tokenize(paragraph):
                if sentence is '':
                    continue
                pill = FactPill(content=sentence, keywords=keywords)
                pill.meta.id = filename+str(keyid)+str(sid)
                try:
                    # print "[INFO] Indexed, ", pill
                    pill.save()
                    logger.debug('[INFO] Indexed keyid= '+pill.meta.id)
                except:
                    print "[DEBUG] Failed indexing: ", pill
                    logger.debug('[ERROR] Failed keyid= '+pill.meta.id)
            return ([], [], keyid+1)
        else:
            pill = FactPill(content=paragraph, keywords=keywords)
            pill.meta.id = filename+str(keyid)
            try:
                # print "[INFO] Indexed, ", pill
                pill.save()
                logger.debug('[INFO] Indexed keyid= '+pill.meta.id)
            except:
                print "[DEBUG] Failed indexing: ", pill
                logger.debug('[ERROR] Failed keyid= '+pill.meta.id)
            return ([], [], keyid+1)
    return ([], [], keyid)

# filename = 'test'
#filename = '20140615-wiki-en_000001.txt.gz'

def parse_wiki(start, end, dirpath):
    for file_index in range(start,end+1): #4633):
        if start<1 or end>4633 or start>end:
            print "[ERROR] Incorrect range of [start,end]=", [start, end]
            sys.exit(122) # random error 122

        filename = dirpath+os.path.sep+"20140615-wiki-en_%06d.txt.gz" % file_index
        keyid = 0 #unique keyid for each factpill in es

        mesg = "[INFO] indexing article " + filename + '...'
        print mesg
        with gzip.open(filename, 'r') as fp:
        #with open(filename, 'r') as fp:
            reader = fp.readlines()
            keywords = []
            paragraph = []
            current_title = ""
        
            for line in reader:
                footer_obj = footer_wiki.match(line)
                title_obj = title_wiki.match(line)
                heading_obj = heading_wiki.match(line)
                if footer_obj is not None:
                    # drop them
                    paratype = 'footer'
                    paragraph, keywords, keyid = index_paragraph(paragraph, paratype, keywords, filename, keyid)
                    keywords = []
                elif title_obj is not None:
                    # add to keywords
                    paratype = 'title'
                    # x = title.match(line)
                    current_title = title_obj.group(1)
                    #print "[DBG] : ", keywords
                elif heading_obj is not None:
                    # add to key words
                    paratype = 'content'
                    paragraph, keywords, keyid = index_paragraph(paragraph, paratype, keywords, filename, keyid)
                    keywords.append(current_title)
                    keywords.append(heading_obj.group(1))
                else:
                    # keep track
                    paragraph.append(line)
        print "done."
        logger.debug(mesg + 'done.')

def parse_dbp(filename):
    keyid = 0 #unique keyid for each factpill in es

    mesg = "[INFO] indexing article " + filename + '...'
    print mesg
    data = pd.read_table(filename, names=['content'])
    if data.iloc[0]['content']=='content':
        data = data.drop(0)
    data = data['content'].apply(cleaned)
    data = data.dropna()

    for index in data.index:
        pill = FactPill(content=data.iloc[index], keywords=[])
        pill.meta.id = filename+str(keyid)
        #print "[INFO] Indexing of ", pill, ' returned status :: ',
        try:
            pill.save()
            # logger.debug('[INFO] Indexed keyid= '+str(keyid))
            keyid = keyid+1
            #print "Success"
        except:
            # print "Failure"
            print "[DEBUG] Failed indexing: ", pill
            # logger.debug('[ERROR] Failed keyid= '+str(keyid))

def parse_ck12(filename):
    keyid = 0 #unique keyid for each factpill in es

    mesg = "[INFO] indexing article " + filename + '...'
    print mesg
    with open(filename, 'r') as fp:
        reader = fp.readlines()
        keywords = []
        paragraph = []
    
        for line in reader:
            stripped_line = line.strip(' \n\r')
            if stripped_line == '':
                if len(paragraph)>0:
                    paratype = 'content'
                    paragraph, keywords, keyid = index_paragraph(paragraph, paratype, keywords, filename, keyid)
                continue
            footer_obj = footer_ck12.match(line)
            heading_obj = heading_ck12.match(line)
            if footer_obj is not None:
                # drop them
                paratype = 'footer'
                paragraph, _, keyid = index_paragraph(paragraph, paratype, keywords, filename, keyid)
            elif heading_obj is not None:
                # add to key words
                paratype = 'content'
                paragraph, keywords, keyid = index_paragraph(paragraph, paratype, keywords, filename, keyid)
                keywords.append(heading_obj.group(1))
            else:
                # keep track
                paragraph.append(line)
    print "done."
    logger.debug(mesg + 'done.')

#if __name__ is '__main__':
try:
    args = docopt(__doc__)
except:
    raise DocoptExit('Error parsing arguments')

def setup_logger(logfile):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.FileHandler(logfile, mode='w+')
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

print args
wiki = args['wiki']
ck12 = args['ck12']
dbp = args['dbp']

ES_CONFIGURATIONS['ES_INDEX']= args['<index_name>']
class FactPill(DocType):
    content = String(analyzer='snowball')
    keywords = String(index='not_analyzed')

    class Meta:
        index = ES_CONFIGURATIONS['ES_INDEX']
        using = elasticsearch_engine
FactPill.init()

        


print "run right", dbp
if wiki is True:
    start = int(args['<start>'])
    end = int(args['<end>'])
    dirpath = args['<path_to_dir>']
    logfile = args['<path_to_log>']+'_'+str(start)+'_'+str(end)
    setup_logger(logfile)

    filename_start = dirpath+os.path.sep+"20140615-wiki-en_%06d.txt.gz" % start
    filename_end = dirpath+os.path.sep+"20140615-wiki-en_%06d.txt.gz" % end
    mesg = 'Beginning to parse wiki files from [' + filename_start + '] to [' + filename_end + '] logging at: ' + logfile
    print mesg
    logger.debug(mesg)
    parse_wiki(start, end, dirpath)
if ck12 is True or dbp is True:
    filename = args['<path_to_file>']
    logfile = args['<path_to_log>']
    setup_logger(logfile)

    mesg = 'Beginning to parse ck12/dbp file from ' + filename + ' logging at: ' + logfile
    print mesg
    logger.debug(mesg)
    if ck12 is True:
        parse_ck12(filename)
    elif dbp is True:
        parse_dbp(filename)

#handler = logging.StreamHandler() # always writes to stdout


