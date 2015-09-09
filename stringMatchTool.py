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
# String matching #
###################

# match_string appears as the first phrase/word in text
def startOfPostMatch(text, match_string):
    match_str_len = len(match_string)
    if len(text) >= match_str_len:
        if text[:match_str_len] == match_string:
            return match_string
    return None
    
# match_string appears in the first sentence
def firstSentenceMatch(text, match_string):
    modtext = text.replace('?','.')
    modtext = modtext.replace('!','.')
    first_sentence = modtext.split('.')[0]
    if match_string in first_sentence:
        return match_string
    return None

# match_string appears anywhere in the text
def anywhereMatch(text, match_string):
    if match_string in text:
        return match_string
    else:
        return None

# make sure use case-insensitive matching method
# upper case letters in front of keys indicate where to look for them in the post
# lowercase both these strings and post text
strings_dict = {
    # A: only at start of post, as the first word/phrase
    'A really, well':       "really, well",
    'A wow':                "wow",
    # B: in the first sentence
    'B oh really':          "oh really",
    # C: can occur anywhere
    'C i guess':            "i guess"
}

# keep track of indices to the functions used to find their match string
match_functions={
    'A':    startOfPostMatch,
    'B':    firstSentenceMatch,
    'C':    anywhereMatch
}

#################################
# General DB-querying functions #
#################################

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
    # format for matching functions, csv output
    text = text.lower().replace('\n',' ')
    text = text.replace('"',"'")
    str_matches = []
    for s in strings_dict:
        if s[0] in match_functions:
            str_match = match_functions[s[0]](text, strings_dict[s])
            if str_match != None:
                str_matches.append(str_match)
    matches = []
    for str_match in str_matches:
        m = Match(disc_id, post_id, str_match, text, parent_id)
        matches.append(m)
    return matches

# given list of match objects, writes to csv
def writeMatchesToCSV(matches, csvfile):
    with open(csvfile,'a', encoding='utf-8') as f:
        for m in matches:
            f.write(str(m.disc_id)+', ')
            f.write(str(m.post_id)+', "')
            f.write(m.str_match+'", "')
            f.write(m.text+'", ')
            f.write(str(m.parent_id)+"\n")

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
    csvfile = "matches_dataset_"+dataset+".csv"

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
            print('Writing matches from post',post_id,'to',post_id+batch_size-1)
            sys.stdout.flush()
            writeMatchesToCSV(matches, csvfile)

    session.close()

if __name__ == "__main__":
    main()
