# -*- coding: utf-8 -*-

import preferences as PREFS

import urllib2
from datetime import *
import json
from optparse import OptionParser

import pymongo

__usage__ = "%prog [-c | --collection]"
__version__ = "%prog v0.1"
__description__ = """
This program is indexer for Solr.
"""
__epilog__ = """
By ContextLogic Korea
"""

def update_index(option, opt_str, value, parser):
	con = pymongo.Connection(host=PREFS.MONGODB['host'], port=PREFS.MONGODB['port'], network_timeout=10)
	print con
	db = con[PREFS.MONGODB['database']]
	coll = db[value]
	print "MongoDB's connection info: %s, count: %s" % (coll, coll.count())
	if value == "product":
		serialize_data = _serialize_products(coll)
	elif value == "opinion":
		serialize_data = _serialize_opinions(coll)
	else:
		con.disconnect()
		print con
		return
	response = _request_update_index(data=serialize_data)
	print response
	con.disconnect()
	print con

def _serialize_products(coll):
	json_list = []
	for doc in coll.find():
		d = doc
		d['id'] = str(d['_id'])
		d['updated'] = datetime.strptime(d['updated'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
		d.pop('_id')
		json_list.append(json.dumps(d))
	return "[%s]" % ", ".join(json_list)

def _serialize_opinions(coll):
	json_list = []
	for doc in coll.find():
		d = doc

		d['id'] = str(d['_id'])
		d.pop('_id')

		if d.has_key("cust_info"):
			d['cust_info_gender'] = d['cust_info']['gender']
			d['cust_info_age'] = d['cust_info']['age']
			d['cust_info_region'] = d['cust_info']['region']
			d.pop('cust_info')

		d['updated'] = datetime.strptime(d['updated'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
		if d['opin_cd'] == "premium":
			d['postdate'] = datetime.strptime(d['postdate'], "%Y.%m.%d").strftime("%Y-%m-%dT00:00:00Z")
			d['feedback_read'] = d['feedback']['read']
			d['feedback_bad'] = d['feedback']['bad']
			d['feedback_good'] = d['feedback']['good']
			d.pop('feedback')
		elif d['opin_cd'] == "normal":
			d['postdate'] = datetime.strptime(d['postdate'][:10], "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")
		else:
			pass

		json_list.append(json.dumps(d))
	return "[%s]" % ", ".join(json_list)

def _request_update_index(data=None):
	req = urllib2.Request(url=PREFS.SOLR['update_index'], headers=PREFS.SOLR['update_header'], data=data)
	print "Solr index update request url: %s" % req.get_full_url()
	print "request header: %s" % req.header_items()
	response = urllib2.urlopen(req)
	return response.read()

def main():
	parser = OptionParser(usage=__usage__, version=__version__, description=__description__, epilog=__epilog__)
	parser.add_option("-c", "--collection", type="string", help="update index to Solr application", action="callback", callback=update_index)
	(options, args) = parser.parse_args()

if __name__ == "__main__":
	main()

