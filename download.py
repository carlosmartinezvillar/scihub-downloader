import os
import xml.etree.ElementTree as ET
import multiprocessing
import requests
import yaml
import numpy as np
import subprocess
import argparse
from tqdm import tqdm

#Global variables for some restrictive parameters
DATA_DIR    = "dat/"
TEMP_DIR    = DATA_DIR + "temp/"
OS_BASE_URI = "https://scihub.copernicus.eu/dhus/search"    #OpenSearch API URI root
OD_BASE_URI = "https://scihub.copernicus.eu/dhus/odata/v1/" #OpenData   API URI root
S2_10_BANDS = ['AOT','B02','B03','B04','B08','TCI','WVP']
S2_20_BANDS = ['AOT','B02','B03','B04','B05','B06','B07','B8A','B11','B12','SCL','TCI','WVP']
S2_60_BANDS = ['AOT','B02','B03','B04','B05','B06','B07','B09','B8A','B11','B12','SCL','TCI','WVP']

#OpenSearch XML Namespaces
NS = {'os':'http://a9.com/-/spec/opensearch/1.1/', 'other':'http://www.w3.org/2005/Atom'}

 #temporary
USER = None 
PASS = None

####################################################################################################

POINT   = "36.0537662 -114.7408903"
POLYGON = "POLYGON((-120.41786863103002 42.17892002081763,-120.90811282563863 41.727878834703404,\
-120.41100158706399 41.38047598342979,-119.93717555340965 41.83884127148377,\
-120.41786863103002 42.17892002081763,-120.41786863103002 42.17892002081763))"
PLATFORMNAME = "Sentinel-2"
PRODUCT      = "S2MSI2A"
START_TIME   = "2020-01-01T00:00:00.000Z"
STOP_TIME    = "2020-12-01T23:59:59:999Z"
RANGE_TIME  = "[%s TO %s]" % (START_TIME,STOP_TIME)
CLOUD_PERCNT = "[0 TO 36]"
BANDS = ["B02","B03","B04","B08"]

params = {
	'coordinates': POINT,
	'platformname': PLATFORMNAME,
	'producttype': PRODUCT,
	'cloudcoverpercentage': CLOUD_PERCNT,
	'beginPosition': RANGE_TIME,
	'endPosition:': RANGE_TIME,
	'startdate': START_TIME,
	'enddate': STOP_TIME,
	'bands': BANDS
}
####################################################################################################
def set_auth_from_env(env_user_str,env_pass_str):
	ENV_USER = os.getenv(env_user_str)
	ENV_PASS = os.getenv(env_pass_str)
	if (ENV_USER is not None) and (ENV_PASS is not None):
		global USER
		global PASS
		USER = ENV_USER
		PASS = ENV_PASS
		print("USER and PASS set to env variables.")


def load_points_from_file(path):
	'''
	Load a comma-separated file in path with each line having a <Lat, Lon> format.
	The  lat, lon order is intended to match ESA's OpenSearch format so that each line can be 
	inserted as is in an openSearch query.
	'''
	with open(path,'r') as fp:
		geom_list = [i.rstrip() for i in fp.readlines()]
	return geom_list


def opensearch_set_query(params):
	'''
	Build the OpenSearch query 
	'''
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
	'''
	Parse a single entry from the results list returned by an OpenSearch query.
	'''
	uuid = xml_entry.find('other:id',namespaces=NS).text

	for s in xml_entry.findall('other:str',NS):
		if s.attrib['name'] == 'footprint':
			footprint = s.text

		if s.attrib['name'] == 'filename':
			filename = s.text

	for d in xml_entry.findall('other:double',NS):
		if d.attrib['name'] == 'waterpercentage':
			waterpercentage = d.text

		if d.attrib['name'] == 'cloudcoverpercentage':
			cloudcoverpercentage = d.text

	return (uuid,filename,waterpercentage,cloudcoverpercentage)


def opensearch_parse_page(root):
	'''
	Parse a single page of results returned by an OpenSearch query 
	'''
	entries_xml = root.findall('other:entry',namespaces=NS)
	entries     = []

	for e in entries_xml:
		row = opensearch_parse_entry(e)
		entries.append(row)

	return np.array(entries)


def opensearch_parse_pages(S,query,n_results):
	'''
	Parse all pages returned by a OpenSearch query and parse them.
	'''
	n_pages = n_results//100 + 1

	for current_page in range(n_pages):
		payload = {'start':current_page,'rows':100,'q':query}
		resp    = S.get(OS_BASE_URI,params=payload)	
		root    = ET.fromstring(resp.text)
		page_results = opensearch_parse_page(root)

		if current_page == 0:
			results = page_results
		else:
			results = np.concatenate([results,page_results],axis=0)

	return results


def opensearch_parse(S,query,n_results):
	'''
	Iterate through all pages returned by OpenSearch query and parse them.	
	'''
	n_pages   = n_results//100 + 1 
	remainder = n_results % 100    #what's left in the last page
	entries   = []

	for current_page in range(n_pages):
		#get page
		payload = {'start':current_page,'rows':100,'q':query}
		resp = S.get(OS_BASE_URI,params=payload)
		root = ET.fromstring(resp.text)

		#Parse -- list of entries
		entries_xml = root.findall('other:entry',namespaces=NS)
		
		#Parse -- each entry in page
		for e in entries_xml:
			uuid = e.find('other:id',NS).text

			for s in e.findall('other:str',NS):
				if s.attrib['name'] == 'footprint': #not used
					footprint = s.text

				if s.attrib['name'] == 'filename':
					filename = s.text

			for d in e.findall('other:double',NS):
				if d.attrib['name'] == 'waterpercentage':
					waterpercentage = d.text

				if d.attrib['name'] == 'cloudcoverpercentage':
					cloudcoverpercentage = d.text

			#append each entry row to a list
			entries += [(uuid,filename,waterpercentage,cloudcoverpercentage)]

	return np.array(entries)


def opensearch_get_header(S,query,params):

	page_start,page_rows,n_results = 0,0,0

	if len(params['coordinates']) < 80:
		print("Coordinates: %s" % params['coordinates'])
	else:
		print("Coordinates: %s" % params['coordinates'][0:65])

	#request
	payload = {'start':0, 'rows':0, 'q':query} #return 0 results
	resp    = S.get(OS_BASE_URI,params=payload)

	#Correct response codes
	assert resp.status_code != 401, "HTTP 401: Unauthorized. Check user and password."
	assert resp.status_code == 200, "opensearch_get_header(): Got code %s." % resp.status_code
	
	#Parse response
	root      = ET.fromstring(resp.text)
	n_results = int(root.find('os:totalResults',namespaces=NS).text)
	n_pages   = n_results//100 + 1

	#Print feedback and return

	print("Found %i results in %i pages." % (n_results,n_pages))
	print("="*100)
	return n_results


def opensearch_multi_coordinate(S,path):
	coords = load_points_from_file(path)

	for c in coords:
		params['coordinates'] = c
		query = opensearch_set_query(params)
		uri   = None



def odata_image_uri(row,band,resolution):
	'''
	Build and return the URI for a single SciHub image file. Input row in table follows the format
	[uuid,filename,footprint,waterpercentage,cloudcoverpercentage,datastrip,granule].
	'''

	#get uri parts from table info
	uuid      = row[0]
	filename  = row[1]
	level     = filename.split('_')[1][3:] #L2A from filename
	tile      = filename.split('_')[-2]
	dstrip    = row[5].split('_')[-2][1:]
	granule   = row[6].split('_')[-3]
	subdir    = "%s_%s_%s_%s" % (level,tile,granule,dstrip)
	ingestion = filename.split('_')[2]
	img_path  = "%s_%s_%s_%s.jp2" % (tile,ingestion,band,resolution)

	#build URI and return
	uri = OD_BASE_URI
	uri += "Products('%s')/"    % uuid
	uri += "Nodes('%s')/"       % filename
	uri += "Nodes('GRANULE')/"
	uri += "Nodes('%s')/"       % subdir
	uri += "Nodes('IMG_DATA')/"
	uri += "Nodes('R%s')/"      % resolution
	uri += "Nodes('%s')/$value" % img_path
	return uri


def odata_mtdxml_uri(row):
	'''
	Build and set the URI for the metadata file of a product given the 
	'''
	#split/get URI elements for table of metadata
	uuid     = row[0]
	filename = row[1]
	level    = filename.split('_')[1][3:]
	tile     = filename.split('_')[-2]

	#build and return
	uri = OD_BASE_URI
	uri += "Products('%s')/" % uuid
	uri += "Nodes('%s')/" % filename
	uri += "Nodes('MTD_MSI%s.xml')/$value" % level 
	return uri


def odata_get_image(S,uri):
	pass


def odata_get_mtdxml(S,row,uri):
	resp = S.get(uri,stream=True)
	total_size = int(resp.headers.get('content-length',0))
	block_size = 1024
	bar = tqdm(total=total_size, unit='iB',unit_scale=True)
	out_file_path = ''
	with open(out_file_path,'wb') as fp:
		for chunk in resp.iter_content(block_size):
			written = fp.write(chunk)
			bar.update(written)
			
	bar.close()
	if total_size != 0 and bar.n != total_size:
		print("Error. Incorrect file size during download.")


def odata_check_status(S,uri):
	'''
	Takes the URI passed to it in uri and sends request to check if product is currently online or 
	not. S is the requests.Session object. Returns bool.
	'''
	resp = S.get(uri)
	if resp.text == 'true':
		return True
	return False


def odata_trigger_offline_request(S,uuid_list):
	for uuid in uuid_list:
		uri = OD_BASE_URI + "Products('%s')/$value" % (uuid)
		#HTTP header 202 means good
		print("Triggering offline request for product %s" % uuid)
		resp = S.get(uri)
		print("Status code: %s" % resp.status_code)


def odata_get_offline_list(S,product_list):
	status_list = []
	for i,row in enumerate(product_list):
		uuid = row[0]
		uri  = OD_BASE_URI + "Products('%s')/Online/$value" % uuid
		print("[%i/%i]" % (i+1,product_list.shape[0]),end=' ')
		print("Checking status of " + uuid, end=' -- ')
		status = odata_check_status(S,uri) #returns true or false
		if status:
			print("Online")
		else:
			print("Offline")
		status_list.append(status)
	
	n_offline = np.array(status_list).sum()
	print("%i/%i products offline" % (n_offline,len(status_list)))

	#numpy append to array as another string col
	product_list[:,-1] = status_list
	product_list[product_list[:,-1] == 'False',-1] = 'offline'
	product_list[product_list[:,-1] == 'True',-1] = 'online'


def parse_mtdxml(path):
	'''
	Get datastrip id and granule id from MTD_MSI
	'''
	pass


def remove_duplicates(product_list):
	'''
	Go through existing numpy array with sentinel product ids and, duh, remove
	duplicates.
	'''
	pass


def write_offline_txt(product_list):
	with open('./dat/offline.txt') as fp:
		fp.write()
	pass


def progress_bar():
	'''
	meh
	'''
	pass


if __name__ == '__main__':
	set_auth_from_env('DHUS_USER','DHUS_PASS')

	#loading stuff
	points = load_points_from_file(DATA_DIR + 'test_sites.txt')
	params['coordinates'] = points[0]

	#Set requests.Session
	S      = requests.Session()
	S.auth = (USER,PASS)
	query  = opensearch_set_query(params)

	#header
	n_results = opensearch_get_header(S,query,params)

	arr = opensearch_parse_multipage(S,query,n_results)
	print(arr[:,1])
	# print(a.shape)

	# S.close()
