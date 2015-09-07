# stringMatchTool.py
# Nicolas Hahn
# NLDS lab
# Given a dataset_id, database login info:
# 	queries the database for all posts in the dataset
# 	checks each for the strings given
# 	outputs a CSV which has on each line:
# 		post id,"regex match","post's text",parent post id 

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

# set these in main()
dataset_id = None

###############
# Match Class #
###############

# each match object, to be written to database
class Match:
	def __init__(self, post_id, string_match, text, parent_id):
		self.post_id = post_id
		self.string_match = string_match
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
	first_sentence = re.split('.?!', text)[0]
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
	# A: only at start of post
	'A really, well':		"really, well",
	# B: in the first sentence
	'B oh really':			"oh really",
	# C: can occur anywhere
	'C wow':				"wow",
	'C i guess':			"i guess",
}

# keep track of indices to the functions used to find their match string
match_functions={
	'A':	startOfPostMatch,
	'B':	firstSentenceMatch,
	'C':	anywhereMatch
}

#################################
# General DB-querying functions #
#################################

# get text, check against strings dict, if there's matches:
# create list of tuples
def getMatchesFromPostID(post_id, session):
	post_text = getTextFromPostID(post_id, session)
	l_text = post_text.lower()
	print(l_text)
	str_matches = []
	for s in strings_dict:
		if s[0] in match_functions:
			str_match = match_functions[s[0]](l_text, strings_dict[s])
		# if s[0] == "A":
		# 	str_match = startOfPostMatch(l_text,strings[s])
		# if s[0] == "B":
		# 	str_match = firstSentenceMatch(l_text,strings[s])
		# if s[0] == "C":
		# 	str_match = anywhereMatch(l_text,strings[s])
			if str_match != None:
				str_matches.append(str_match)
	if len(str_matches) == 0:
		return []
	else:
		parent_id = getParentFromPostID(post_id,session)
		new_matches = []
		for str_match in str_matches:
			new_matches.append(Match(post_id, str_match, post_text, parent_id))
		return new_matches
		

# given a post_id, returns body of text of that post
def getTextFromPostID(post_id, session):
	# query posts for text_id
	pquery = session.query(Post).\
				filter(Post.post_id.like(post_id)).\
				filter(Post.dataset_id.like(dataset_id))
	text_id = pquery[0].text_id
	# now get text from text_id
	tquery = session.query(Text).\
				filter(Text.text_id.like(text_id)).\
				filter(Text.dataset_id.like(dataset_id))
	return tquery[0].text

# given post_id, return parent_post_id
def getParentFromPostID(post_id, session):
	pquery = session.query(Post).\
				filter(Post.post_id.like(post_id)).\
				filter(Post.dataset_id.like(dataset_id))
	return pquery[0].parent_post_id

# given list of match objects, writes to csv
def writeMatchesToCSV(matches, csvfile):
	with open(csvfile,'a') as f:
		for m in matches:
			f.write(str(m.post_id)+', "')
			f.write(m.string_match+'", "')
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
	csvfile = "matches_dataset_"+dataset+".csv"
	matches = []

	# query db for # of posts
	totalPosts = None
	for ct in session.query(
			func.count(Post.post_id)).\
			filter(Post.dataset_id.like(dataset_id)):
		totalPosts = ct[0]
		print('total number of posts in dataset:',totalPosts)
		sys.stdout.flush()

	# generate the match objs by iterating through post_ids
	for post_id in range(totalPosts):
		post_id += 1
		# if post_id%1000 == 0:
		print("at post",post_id)
		sys.stdout.flush()
		newMatches = getMatchesFromPostID(post_id, session)
		for nm in newMatches:
			matches.append(nm)
		if len(matches) >= 1000:
			print('Writing matches for up to post_id',post_id)
			sys.stdout.flush()
			writeMatchesToCSV(matches, csvfile)
			matches = []
	writeMatchesToCSV(matches, csvfile)
	session.close()

if __name__ == "__main__":
	main()
