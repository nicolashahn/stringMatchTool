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
import sys
import re

#############################
# Database connection/setup #
#############################

# open connection to database
# then return engine object
def connect(username, password, database):
	db_uri = 'mysql+oursql://{}:{}@127.0.0.1:{}'.format(username, password, database)
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

# creates a table class, autoincrementer for each table in the database
def generateTableClasses(eng):
	ABase = automap_base()
	ABase.prepare(eng, reflect=True)
	global Post, Text
	Post = ABase.classes.posts
	Text = ABase.classes.texts

####################
# Strings to match #
####################

# make sure use case-insensitive matching method
# upper case letters in front of keys indicate where to look for them in the post
strings = {
	# only at start of post
	'Areallywell':		"really, well",
	# in the first sentence
	'Bohreally':		"oh really",
	# can occur anywhere
	'Cwow':				"wow",
	'Ciguess':			"i guess",
}

##################
# Main Execution #
##################

def main():
	print('Connecting to database',db,'as user',user)
	eng = connect(user, pword, db)
	metadata = s.MetaData(bind=eng)
	session = createSession(eng)

if __name__ == "__main__":
	main()
