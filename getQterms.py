#!/opt/python27/bin/python
import argparse
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser()
parser.add_argument("queryfile")
parser.add_argument("stop_file")
parser.add_argument("--xml","-x", type=int, default=0)
args = parser.parse_args()

termset = {}
if not args.xml:
	stoplist = set()
	for line in open(args.stop_file):
		stoplist.add(line.strip())
	for line in open(args.queryfile):
		terms = line.split()
		for t in terms:
			if t.isalpha() and t not in stoplist:
				termset[t] = termset.get(t, 0) + 1
	
	t2 = sorted([(v,k) for k, v in termset.items()], reverse=True)

	for f, t in t2:
		print t + " " + str(f)

else:
	tree = ET.parse(args.queryfile)
	root = tree.getroot()
	for query in root:
		text = query.find('text').text
		terms = text.split()
		for t in terms:
			print t
