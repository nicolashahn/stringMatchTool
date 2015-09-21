# stringMatchTool.py
# Nicolas Hahn
# NLDS lab
# Given a dataset_id, database login info:
#   queries the database for all posts in the dataset
#   checks each for the strings given
#   outputs a CSV which has on each line:
#       post id,"regex match","post's text",parent post id 
# NOTE:
#   modified for dataset 3: added discussion_id as first field
#   posts are only unique to the discussion

import sqlalchemy as s
import oursql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.dialects import mysql
from sqlalchemy import func
import sys
import re
import os

###########
# Globals #
###########

# set this in main()
dataset_id = None
# how many posts to get at once
batch_size = 50000

# files with regex patterns on each line to look for
# by default, will search entire text for each pattern
# regex_files = [
#     "LIWC_friend_enum.txt",
# ]

###############
# Match Class #
###############

# each match object, to be written to database
class Match:
    def __init__(self, 
        disc_id=None, 
        post_id=None, 
        str_match=None, 
        text=None, 
        parent_id=None,
        parent_text=None):
        self.disc_id = disc_id
        self.post_id = post_id
        self.str_match = str_match
        self.text = text
        self.parent_id = parent_id
        self.parent_text = parent_text

#############################
# Database connection/setup #
#############################

# open connection to database
# then return engine object
def connect(username, password, database):
    db_uri = 'mysql+oursql://{}:{}@{}'.format(username, password, database)
    # db_uri = 'mysql://{}:{}@{}'.format(username, password, database)
    engine = s.create_engine(db_uri, encoding='utf-8')
    engine.connect()
    return engine

# create a session from the engine
def createSession(eng):
    Session = s.orm.sessionmaker()
    Session.configure(bind=eng)
    session = Session()
    return session

# creates a table class for each table used
def generateTableClasses(eng):
    ABase = automap_base()
    ABase.prepare(eng, reflect=True)
    global Post, Text
    Post = ABase.classes.posts
    Text = ABase.classes.texts

###################
# Regex matching  #
###################

# match_regex appears as the first phrase/word in text
def startOfPostMatch(text, match_regex):
    start_match = r"^"+match_regex
    matches = re.findall(start_match, text)
    if len(matches)>0:
        return matches[0]
    return None
    
# match_regex appears in the first sentence
# minus the punctuation - use startOfPostMatch if you want that
def firstSentenceMatch(text, match_regex):
    modtext = text.replace('?','.')
    modtext = modtext.replace('!','.')
    first_sentence = modtext.split('.')[0]
    matches = re.findall(match_regex, first_sentence)
    if len(matches)>0:
        return matches[0]
    return None

# match_regex appears anywhere in the text
def anywhereMatch(text, match_regex):
    matches = re.findall(match_regex, text)
    if len(matches)>0:
        return matches[0]
    return None

# match_regex appears in the first X words of the text
# def firstXWordsMatch(text, numWords, match_regex):
#     splitText = text.split(' ')
#     firstXWords = ' '.join(splitText[:numWords])
#     if match_regex in firstXWords:
#         return match_regex
#     else:
#         return None

# first char = position in text to look (see match_functions below)
# rest of key = name of regex phrase
# value = regex pattern
regex_dict = {
    # A: only at start of post, as the first word/phrase
    # B: in the first sentence
    # C: can occur anywhere

    'A really?':           r"(really\?)",
    'A wow':               r"(wow)",
    'A interesting.':      r"(interesting\.)",
    'A interestingly,':    r"(interestingly,)",
    
    'B oh really':         r"(oh really)",
    'B i love':            r"(i love)",
    
    'C i guess':           r"(i guess)",
    "C you're kidding":    r"(you're kidding)",
    "C you're joking":     r"(you're joking)",
    "C fantastic":         r"(fantastic)",
}


# keep track of indices to the functions used to find their match string
match_functions={
    'A':    startOfPostMatch,
    'B':    firstSentenceMatch,
    'C':    anywhereMatch,
    # 'D':    firstXWordsMatch
}

# find files with regex patterns to use
def findRegexFiles(dir):
    regex_files = []
    for file in os.listdir(dir):
        regex_files.append(file)
    return regex_files

# loads regex patterns from LIWC regex files
def addRegexFromFile(filename):
    with open(filename,'r') as f:
        for line in f:
            r = line.rstrip()
            r.replace("\n","")
            regex_dict["C "+r] = "("+r+")"

#################################
# General DB-querying functions #
#################################

# for output to CSV and string matching
def cleanText(text):
    newtext = text.lower().replace('\n',' ')
    newtext = newtext.replace('"',"'")
    return newtext

# query the db once per batch, then get matches
def getBatchMatches(post_id, session):
    pquery = session.query(Post,Text).\
                filter((Post.dataset_id==dataset_id) &
                        (Text.dataset_id==dataset_id) & 
                        (Post.text_id==Text.text_id)).\
                limit(batch_size).offset(post_id)
    matches = []
    for p,t in pquery.all():
        if t.text:
            if len(cleanText(t.text).split(' ')) in range(10,151):
                m = getMatchesFromText(
                        p.discussion_id, 
                        p.post_id, 
                        t.text, 
                        p.parent_post_id
                        )
                matches += m
    return matches

# def getBatchMatches(post_id, session):
#     pquery = session.query(Post).\
#                 filter(Post.dataset_id==dataset_id).\
#                 limit(batch_size).offset(post_id)
#     matches = []
#     for p in pquery.all():
#         m = Match(disc_id=p.discussion_id, post_id=p.post_id, text_id=p.text_id)
#         matches.append(m)
#     return matches


# check for each of the strings in the string_dict
# if exists, create a Match object
def getMatchesFromText(disc_id, post_id, text, parent_id):
    newtext = cleanText(text)
    r_matches = []
    for r in regex_dict:
        if r[0] in match_functions:
            r_match = match_functions[r[0]](newtext, regex_dict[r])
            if r_match != None:
                r_matches.append(r_match)
    matches = []
    for r_match in r_matches:
        m = Match(disc_id, post_id, r_match, newtext, parent_id)
        matches.append(m)
    return matches

# get the parent's text from the parent_id in match objects
def addParentText(matches, session):
    disc_ids = set([m.disc_id for m in matches if m.parent_id is not None])
    # get way more posts than we need, whittle down
    pquery = session.query(Post).\
                filter(Post.dataset_id==dataset_id).\
                filter(Post.discussion_id.in_(disc_ids))
    # now have a list of all posts from all discussions that might be relevant
    pd_text_ids = {}
    for p in pquery.all():
        pd_text_ids[(p.post_id, p.discussion_id)] = p.text_id
    # fish out relevant text_ids
    relevant_text_ids = []
    for m in matches:
        if (m.parent_id, m.disc_id) in pd_text_ids:
            relevant_text_ids.append(pd_text_ids[(m.parent_id,m.disc_id)])
    # now actually get the texts
    tquery = session.query(Text).\
                filter(Text.dataset_id==dataset_id).\
                filter(Text.text_id.in_(relevant_text_ids))
    t_id_texts = {}
    for t in tquery.all():
        t_id_texts[t.text_id] = t.text
    # link them up with their respective Match objects
    for m in matches:
        if (m.parent_id, m.disc_id) in pd_text_ids:
            ptext = t_id_texts[pd_text_ids[(m.parent_id, m.disc_id)]]
            m.parent_text = cleanText(ptext)
    return matches

# some posts are duplicated, IE posts with different post_id and discussion_id
# have the same exact text
# let's remove the ones that don't have parent texts,
# if both do or don't, just pick one to remove
def removeDuplicateTexts(matches):
    matches = sorted(matches, key=lambda k:k.text)
    origLen = len(matches)
    for i in range(origLen-1):
        while i < len(matches)-1:
            if matches[i].text == matches[i+1].text:
                if matches[i].parent_id == None:
                    del matches[i]
                else:
                    del matches[i+1]
            else:
                break
    return matches


# given list of match objects, writes to csv
def writeMatchesToCSV(matches, csvfile):
    with open(csvfile,'a', encoding='utf-8') as f:
        for m in matches:
            f.write('"'+str(m.disc_id)+'",')
            f.write('"'+str(m.post_id)+'",')
            f.write('"'+m.str_match+'",')
            f.write('"'+m.text+'",')
            f.write('"'+str(m.parent_id)+'",')
            f.write('"'+str(m.parent_text)+'"')
            f.write('\n')
    

##################
# Main Execution #
##################

def main(user=sys.argv[1],pword=sys.argv[2],db=sys.argv[3],dataset=sys.argv[4]):

    # usual stuff to sync with MySQL db, setup
    print('Connecting to database',db,'as user',user)
    sys.stdout.flush()
    eng = connect(user, pword, db)
    metadata = s.MetaData(bind=eng)
    session = createSession(eng)
    generateTableClasses(eng)
    global dataset_id
    dataset_id = int(dataset)
    matches = []
    
    regex_files = findRegexFiles('/LIWC_lexicon')

    for f in regex_files:
        addRegexFromFile(f)
    print (regex_dict)
    
    # what file to write to
    csvfile = "matches_regex_dataset_"+dataset+".csv"
    with open(csvfile,'w',encoding='utf-8') as f:
        f.write('"discussion_id","post_id","string matched","post text","parent_post_id"\n')
    
    # query db for # of posts
    totalPosts = None
    for ct in session.query(
            func.count(Post.post_id)).\
            filter(Post.dataset_id.like(dataset_id)):
        totalPosts = ct[0]
        print('total number of posts in dataset:',totalPosts)
        sys.stdout.flush()

    for post_id in range(totalPosts):
        if post_id%batch_size == 0:
            matches = getBatchMatches(post_id, session)
            matches = removeDuplicateTexts(matches)
            matches = addParentText(matches, session)
            print('Writing matches from post',post_id+1,'to',post_id+batch_size)
            sys.stdout.flush()
            writeMatchesToCSV(matches, csvfile)

    session.close()

if __name__ == "__main__":
    main()
