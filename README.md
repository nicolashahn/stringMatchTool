# stringMatchTool
Usage:
```
python3 stringMatchTool.py username password server/database dataset_id
```
Checks through all the posts in the dataset for certain strings:

- "Wow" at the beginning of a post.
- "I guess" anywhere in a post
- "Oh really" near the beginning of a post, probably at the beginning.
- "Really, well" at the beginning of a post.

and outputs a CSV file with the following on each line:
```
discussion id, post id, "string match", "post's text", parent post id 
```

Should be easily extensible for other phrases/positions in text.

Requires sqlalchemy, oursql.
