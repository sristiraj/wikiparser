import re
import json
import pprint


parseddict = []
with open("wiki_sample_output.txt") as f:
    # print(f.readlines())
    data = f.read()
  
groups = data.split("\n\n\n")

title = groups[0]
for group in groups[1:]:
    author = group[:group.find("\n")]
    paragraph = group[group.find("\n")+1:]
    parseddict.append({"title":title,"author":author,"paragraph":paragraph})

print(json.dumps(parseddict,sort_keys=True, indent=1))