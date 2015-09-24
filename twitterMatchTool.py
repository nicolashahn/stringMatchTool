# twitterMatchTool.py
# Nicolas Hahn
# NLDS lab
# Twitter version of regexMatchTool.py
# Given a dataset_id, database login info:
#   queries the database for all tweets in the dataset
#   checks each for the strings given
#   outputs a CSV which has on each line:
#       tweet id,"regex match","tweet's text",parent tweet id 
# NOTE:
#   modified for dataset 3: added discussion_id as first field
#   tweets are only unique to the discussion

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
dataset_id = 7
# how many tweets to get at once
batch_size = 100

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
        tweet_id=None, 
        str_match=None, 
        text=None,
        parent_id=None, 
        parent_text=None
        ):
        self.tweet_id = tweet_id
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
    global Tweet, Text
    Tweet = ABase.classes.tweets
    Text = ABase.classes.texts

###################
# Regex matching  #
###################

# match_regex appears as the first phrase/word in text
def startOfTweetMatch(text, match_regex):
    start_match = r"^"+match_regex
    matches = re.findall(start_match, text)
    if len(matches)>0:
        return matches[0]
    return None
    
# match_regex appears in the first sentence
# minus the punctuation - use startOfTweetMatch if you want that
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
    # A: only at start of tweet, as the first word/phrase
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
    'A':    startOfTweetMatch,
    'B':    firstSentenceMatch,
    'C':    anywhereMatch,
    # 'D':    firstXWordsMatch
}

# find files with regex patterns to use
def findRegexFiles(dir):
    regex_files = []
    for file in os.listdir(dir):
        regex_files.append(dir+'/'+file)
    return regex_files

# loads regex patterns from LIWC regex files
def addRegexFromFile(filename):
    with open(filename,'r') as f:
        for line in f:
            r = line.rstrip()
            r = r.replace("*",".*")
            regex_dict["C "+r] = '\W('+r+')\W' 

#################################
# General DB-querying functions #
#################################

# for output to CSV and string matching
def cleanText(text):
    if text != None:
        newtext = text.lower().replace('\n',' ')
        newtext = newtext.replace('"',"'")
        return newtext

# query the db once per batch, then get matches
def getBatchMatches(tweet_id, session):
    pquery = session.query(Tweet,Text).\
                filter((Tweet.dataset_id==dataset_id) &
                        (Text.dataset_id==dataset_id) & 
                        (Tweet.text_id==Text.text_id)).\
                limit(batch_size).offset(tweet_id)
    matches = []
    for p,t in pquery.all():
        if t.text:
            if len(cleanText(t.text).split(' ')) in range(10,151):
                m = getMatchesFromText(p.tweet_id, t.text, p.in_reply_to_tweet_id)
                matches += m
    return matches

# check for each of the strings in the string_dict
# if exists, create a Match object
def getMatchesFromText(tweet_id, text, parent_id):
    newtext = cleanText(text)
    r_matches = []
    for r in regex_dict:
        if r[0] in match_functions:
            r_match = match_functions[r[0]](newtext, regex_dict[r])
            if r_match != None:
                r_matches.append(r_match)
    matches = []
    for r_match in r_matches:
        m = Match(tweet_id, r_match, newtext, parent_id)
        matches.append(m)
    return matches

# get the parent's text from the in_reply_to_tweet_id in match objects
def addParentText(matches, session):
    # disc_ids = set([m.disc_id for m in matches if m.parent_id is not None])
    parent_ids = set([m.parent_id for m in matches if m.parent_id is not None])
    pquery = session.query(Tweet).\
                filter(Tweet.dataset_id==dataset_id).\
                filter(Tweet.tweet_id.in_(parent_ids))
    pd_text_ids = {}
    relevant_text_ids = []
    for p in pquery.all():
        pd_text_ids[p.tweet_id] = p.text_id
        relevant_text_ids.append(p.text_id)
    # fish out relevant text_ids
    # relevant_text_ids = []
    # for m in matches:
    #     if parent_id in pd_text_ids:
    #         relevant_text_ids.append(pd_text_ids[(m.parent_id,m.disc_id)])
    # now actually get the texts
    tquery = session.query(Text).\
                filter(Text.dataset_id==dataset_id).\
                filter(Text.text_id.in_(relevant_text_ids))
    t_id_texts = {}
    for t in tquery.all():
        t_id_texts[t.text_id] = t.text
    # link them up with their respective Match objects
    for m in matches:
        if m.parent_id in pd_text_ids:
            ptext = t_id_texts[pd_text_ids[m.parent_id]]
            m.parent_text = cleanText(ptext)
    return matches

# some tweets are duplicated, IE tweets with different tweet_id and discussion_id
# have the same exact text
# let's remove the ones that don't have parent texts,
# if both do or don't, just pick one to remove
# def removeDuplicateTexts(matches):
#     matches = sorted(matches, key=lambda k:k.text)
#     origLen = len(matches)
#     for i in range(origLen-1):
#         while i < len(matches)-1:
#             if matches[i].text == matches[i+1].text:
#                 if matches[i].parent_id == None:
#                     del matches[i]
#                 else:
#                     del matches[i+1]
#             else:
#                 break
#     return matches


# given list of match objects, writes to csv
def writeMatchesToCSV(matches, csvfile):
    with open(csvfile,'a', encoding='utf-8') as f:
        for m in matches:
            # f.write('"'+str(m.disc_id)+'",')
            f.write('"'+str(m.tweet_id)+'",')
            f.write('"'+m.str_match+'",')
            f.write('"'+m.text+'",')
            f.write('"'+str(m.parent_id)+'",')
            f.write('"'+str(m.parent_text)+'"')
            f.write('\n')
    

##################
# Main Execution #
##################

def main(user=sys.argv[1],pword=sys.argv[2],db=sys.argv[3]):

    # usual stuff to sync with MySQL db, setup
    print('Connecting to database',db,'as user',user)
    sys.stdout.flush()
    eng = connect(user, pword, db)
    metadata = s.MetaData(bind=eng)
    session = createSession(eng)
    generateTableClasses(eng)
    matches = []
    
    regex_files = findRegexFiles('LIWC_lexicons')

    for f in regex_files:
        addRegexFromFile(f)
    print (regex_dict)
    
    # what file to write to
    csvfile = "matches_regex_dataset_"+str(dataset_id)+".csv"
    with open(csvfile,'w',encoding='utf-8') as f:
        f.write('"discussion_id","tweet_id","string matched","tweet text"\n')
    
    # query db for # of tweets
    totalTweets = None
    for ct in session.query(
            func.count(Tweet.tweet_id)).\
            filter(Tweet.dataset_id.like(dataset_id)):
        totalTweets = ct[0]
        print('total number of tweets in dataset:',totalTweets)
        sys.stdout.flush()

    for tweet_id in range(totalTweets):
        if tweet_id%batch_size == 0:
            matches = getBatchMatches(tweet_id, session)
            # matches = removeDuplicateTexts(matches)
            matches = addParentText(matches, session)
            print('Writing matches from tweet',tweet_id+1,'to',tweet_id+batch_size)
            sys.stdout.flush()
            writeMatchesToCSV(matches, csvfile)

    session.close()

if __name__ == "__main__":
    main()
