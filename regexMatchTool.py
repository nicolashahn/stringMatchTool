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

###########
# Globals #
###########

# set this in main()
dataset_id = None
# how many posts to get at once
batch_size = 10000

###############
# Match Class #
###############

# each match object, to be written to database
class Match:
    def __init__(self, disc_id, post_id, str_match, text, parent_id):
        self.disc_id = disc_id
        self.post_id = post_id
        self.str_match = str_match
        self.text = text
        self.parent_id = parent_id
        # this gets added later, so don't have it in constructor
        self.parent_text = None

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

# first char = position in text to look
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
                filter(Post.dataset_id==dataset_id).\
                filter(Post.text_id==Text.text_id).\
                limit(batch_size).offset(post_id)
    matches = []
    for p,t in pquery.all():
        m = getMatchesFromText(p.discussion_id, p.post_id, t.text, p.parent_post_id)
        matches = matches + m
    return matches

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

# TODO 9/10/2015: change query to get parent text, is currently broken
# get the parent's text from the parent_id in match objects
def addParentText(matches, session):
    parent_id_disc_id = {}
    for m in matches:
        if m.parent_id is not None:
            parent_id_disc_id[m.parent_id] = m.disc_id
    pquery = session.query(Post).\
                filter(Post.dataset_id==dataset_id).\
                filter(
                    Post.post_id.in_(list(parent_id_disc_id)) and 
                    parent_id_disc_id[Post.post_id]==Post.discussion_id)
    print(len(pquery.all()))
    # dict: parent's post_id to its text
    # post_id_text = []
    # for p,t in pquery.all():
    #     print(p.post_)
    #     text = cleanText(t.text)
    #     post_id_text[p.post_id] = text
    # for m in matches:
    #     if m.parent_id is not None and m.parent_id in post_id_text:
    #         m.parent_text = post_id_text[m.parent_id]
    # return matches

# given list of match objects, writes to csv
def writeMatchesToCSV(matches, csvfile):
    with open(csvfile,'a', encoding='utf-8') as f:
        for m in matches:
            f.write('"'+str(m.disc_id)+'",')
            f.write('"'+str(m.post_id)+'",')
            f.write('"'+m.str_match+'",')
            f.write('"'+m.text+'",')
            f.write('"'+str(m.parent_id)+'"')
            # f.write('"'+str(m.parent_text)+'"')
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
            # matches = addParentText(matches, session)
            print('Writing matches from post',post_id,'to',post_id+batch_size-1)
            sys.stdout.flush()
            writeMatchesToCSV(matches, csvfile)

    session.close()

if __name__ == "__main__":
    main()
