import os
import xml.etree.ElementTree as ET
import multiprocessing
import requests
import yaml
import numpy as np
import subprocess

DATA_DIR    = "data/"
TEMP_DIR    = DATA_DIR + "temp/"
OS_BASE_URI = "https://scihub.copernicus.eu/dhus/search"    #OpenSearch API URI root
OD_BASE_URI = "https://scihub.copernicus.eu/dhus/odata/v1/" #OpenData   API URI root
S2_10_BANDS = ['AOT','B02','B03','B04','B08','TCI','WVP']
S2_20_BANDS = ['AOT','B02','B03','B04','B05','B06','B07','B8A','B11','B12','SCL','TCI','WVP']
S2_60_BANDS = ['AOT','B02','B03','B04','B05','B06','B07','B09','B8A','B11','B12','SCL','TCI','WVP']

ENV_USER = os.getenv('DHUS_USER') #temporary
ENV_PASS = os.getenv('DHUS_PASS')
USER = None 
PASS = None

#XML Namespaces
NS = {'os':'http://a9.com/-/spec/opensearch/1.1/', 'other':'http://www.w3.org/2005/Atom'}

####################################################################################################

POINT   = "36.0537662 -114.7408903"
POLYGON = "POLYGON((-120.41786863103002 42.17892002081763,-120.90811282563863 41.727878834703404,\
-120.41100158706399 41.38047598342979,-119.93717555340965 41.83884127148377,\
-120.41786863103002 42.17892002081763,-120.41786863103002 42.17892002081763))"
PLATFORMNAME = "Sentinel-2"
PRODUCT      = "S2MSI2A"
START_TIME   = "2019-01-01T00:00:00.000Z"
STOP_TIME    = "2020-12-01T23:59:59:999Z"
range_time   = "[%s TO %s]" % (START_TIME,STOP_TIME)
CLOUD_PERCNT = "[0 TO 36]"
BANDS = ["B02","B03","B04","B08"]

params = {
	'coordinates': POINT,
	'platformname': PLATFORMNAME,
	'producttype': PRODUCT,
	'cloudcoverpercentage': CLOUD_PERCNT,
	'beginPosition': range_time,
	'endPosition:': range_time,
	'startdate':START_TIME,
	'enddate':STOP_TIME,
	'bands': BANDS
}

####################################################################################################
def set_auth_from_env():
	if (ENV_USER is not None) and (ENV_PASS is not None):
		global USER
		global PASS
		USER = ENV_USER
		PASS = ENV_PASS
		print("USER and PASS set to env variables.")

def points_from_file(path):
	'''
	Load file in path with each line corresponding to a point. Each point must follow the Lat Lon       
	order so that each line in the file is a string matching the ESA's SciHub OpenSearch format.  
	Each line can be inserted as is in a string "Intersects(%s)".
	'''
	with open(path,'r') as fp:
		geom_list = [i.rstrip() for i in fp.readlines()]
	return geom_list

def opensearch_set_query(params):
	query = 'footprint:\"Intersects(%s)\"' % params['coordinates']
	query += ' AND '
	query += 'platformname:%s' % params['platformname']
	query += ' AND '
	query += 'producttype:%s' % params['producttype']
	query += ' AND '
	query += 'beginPosition:[%s TO %s]' % (params['startdate'],params['enddate'])
	query += ' AND '
	query += 'endPosition:[%s TO %s]' % (params['startdate'],params['enddate'])
	query += ' AND '
	query += 'cloudcoverpercentage:%s' % params['cloudcoverpercentage']
	return query

def opensearch_parse_entry(xml_entry):
	#id
	#id_tstamp
	#id_tile
	#filename
	#uuid
	#footprint
	#waterpercentage
	#vegetationpercentage
	pass

def opensearch_parse_list(query,S,n_results):

	n_pages   = n_results//100 + 1
	remainder = n_results % 100

	entry_array = []

	#TODO -- This in FOR-LOOP
	for current_page in range(n_pages):

		payload = {'start':current_page,'rows':100,'q':query}
		resp = S.get(OS_BASE_URI,params=payload)
		root = ET.fromstring(resp.text)

		entries = root.findall('other:entry',namespaces=NS)
		
		for e in entries:
			uuid = e.find('other:id',NS).text

			for s in e.findall('other:str',NS):
				if s.attrib['name'] == 'footprint':
					footprint = s.text

				if s.attrib['name'] == 'filename':
					filename = s.text


			for d in e.findall('other:double',NS):
				if d.attrib['name'] == 'waterpercentage':
					waterpercentage = d.text

				if d.attrib['name'] == 'cloudcoverpercentage':
					cloudcoverpercentage = d.text

			entry_array += [(uuid,filename,footprint,waterpercentage,cloudcoverpercentage)]



	return np.array(entry_array)

def opensearch_get_header(query,S):
	page_start,page_rows,n_results = 0,0,0
	# if method == 'curl':
	# 	q = urllib.parse.quote(query,safe=',()/[]:')
	# 	payload = OS_BASE_URI + '?start=' + str(page_start) + '&rows=' + str(page_rows) + '&q=' + q
	# 	userpass = USER + ':' + PASS
	# 	resp = subprocess.run(['curl', '-u',userpass,payload],stdout=subprocess.PIPE,text=True)
	# 	root = ET.fromstring(resp)

	payload = {'start':page_start, 'rows':page_rows, 'q':query} #return 0 results
	resp = S.get(OS_BASE_URI,params=payload)	
	# resp    = requests.get(OS_BASE_URI, auth=(USER,PASS),params=payload)

	assert resp.status_code != 401, "Got HTTP 401: Unauthorized. Check user and password."
	assert resp.status_code == 200, "opensearch_get_header(): Got code %s." % resp.status_code
	
	root = ET.fromstring(resp.text)
	n_results = int(root.find('os:totalResults',NS).text)

	# print("Coordinates %s")
	n_pages = n_results//100 + 1 
	print("Found %i results in %i pages." % (n_results,n_pages))
	return n_results

def odata_image_uri():
	pass

def odata_mtdxml_uri():
	pass

def odata_get_image():
	pass

def odata_get_mtdxml():
	pass

def odata_check_status():
	pass

def odata_trigger_offline_request(uuid_list):
	for uuid in uuid_list:
		uri = OD_BASE_URI + "Products('%s')/$value" % (uuid)
		#HTTP header 202 is success


def odata_get_offline_list():
	pass


if __name__ == '__main__':
	pass

	#loading stuff
	points = points_from_file('./dat/nevada_centroid.txt')
	params['coordinates'] = points[10]
	query = opensearch_set_query(params)
	set_auth_from_env()

	#Set requests.Session
	S      = requests.Session()
	S.auth = (USER,PASS)

	#header
	n_results = opensearch_get_header(query,S)
	# n_pages   = n_results//100 + 1
	# remainder = n_results % 100
	# print("Coordinates %s" % params['coordinates'])
	# print("Found %i results in %i pages." % (n_results,n_pages))

	a = opensearch_parse_list(query,S,n_results)
	print(a[:,0:2])
	print(a.shape)

	S.close()
