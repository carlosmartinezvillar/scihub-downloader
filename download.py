import os
import xml.etree.ElementTree as ET
import multiprocessing
import requests
import yaml
import numpy as np
import subprocess
from tqdm import tqdm

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
	order so that each line in the file is a string matching ESA's SciHub OpenSearch format. In
	other words, each line can be inserted-as is-in the "Intersects(%s)" section of the openSearch
	query.
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
	'''
	unused
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

	return (uuid,filename,footprint,waterpercentage,cloudcoverpercentage)


def opensearch_parse_list(S,query,n_results):
	n_pages   = n_results//100 + 1
	remainder = n_results % 100
	entry_array = []


	for current_page in range(n_pages):

		#get page
		payload = {'start':current_page,'rows':100,'q':query}
		resp = S.get(OS_BASE_URI,params=payload)
		root = ET.fromstring(resp.text)

		#Parse -- get list
		entries = root.findall('other:entry',namespaces=NS)
		
		#Parse each entry
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

			#append each entry to array
			entry_array += [(uuid,filename,footprint,waterpercentage,cloudcoverpercentage)]

	return np.array(entry_array)


def opensearch_get_header(S,query,params):

	page_start,page_rows,n_results = 0,0,0

	#request

	payload = {'start':0, 'rows':0, 'q':query} #return 0 results
	resp = S.get(OS_BASE_URI,params=payload)	

	#Correct response codes
	assert resp.status_code != 401, "Got HTTP 401: Unauthorized. Check user and password."
	assert resp.status_code == 200, "opensearch_get_header(): Got code %s." % resp.status_code
	
	#Parse response
	root = ET.fromstring(resp.text)
	n_results = int(root.find('os:totalResults',namespaces=NS).text)
	n_pages = n_results//100 + 1

	#Print results adn return
	print("For coordinates %s" % params['coordinates'])
	print("Found %i results in %i pages." % (n_results,n_pages))
	return n_results


def odata_image_uri(table_row,bands):
	uuid      = table_row[0]
	filename  = table_row[1]
	level     = filename.split('_')[1][3:] #L2A from filename
	tile      = filename.split('_')[-2]
	dstrip    = table_row[5].split('_')[-2][1:]
	granule   = table_row[6].split('_')[-3]
	subdir    = "%s_%s_%s_%s" % (level,tile,granule,dstrip)
	ingestion = filename.split('_')[2]

	uris = []

	for band in bands:
		img_path  = "%s_%s_%s_10m.jp2" % (tile,ingestion,band)

		uri = OD_BASE_URI
		uri += "Products('%s')/" % uuid
		uri += "Nodes('%s')/" % filename
		uri += "Nodes('GRANULE')/"
		uri += "Nodes('%s')/" % subdir
		uri += "Nodes('IMG_DATA')/"
		uri += "Nodes('R10m')/"
		uri += "Nodes('%s')/$value" % img_path

		uris += [uri]

	return uris


def odata_mtdxml_uri(table_row):
	#split/get URI elements for table of metadata
	uuid     = table_row[0]
	filename = table_row[1]
	level    = file.split('_')[1][3:]
	tile     = file.split('_')[-2]
	dstrip   = table_row[5].split('_')[-2][1:] 
	granule  = table_row[6].split('_')[-3]

	#build and return
	uri = OD_BASE_URI
	uri += "Products('%s')/" % uuid
	uri += "Nodes('%s')/" % filename
	uri += "Nodes('MTD_MSI%s.xml')/$value" % level 
	return uri

def odata_get_image():
	pass


def odata_get_mtdxml(S,table_row,uri):
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
	resp = S.get(uri)
	if resp.text == 'true':
		return True
	return False


def odata_trigger_offline_request(S,uuid_list):
	for uuid in uuid_list:
		uri = OD_BASE_URI + "Products('%s')/$value" % (uuid)

		#HTTP header 202 means good


def odata_get_offline_list(S,product_list):
	for row in product_list:
		uri = odata_mtdxml_uri(row)
		odata_check_status(S,uri)



def parse_mtd_xml(path):
	pass


def remove_duplicates(product_list):
	pass


def progress_bar():
	pass


if __name__ == '__main__':
	set_auth_from_env()

	#loading stuff
	points = points_from_file('./dat/nevada_centroid.txt')
	params['coordinates'] = points[10]

	#Set requests.Session
	S      = requests.Session()
	S.auth = (USER,PASS)
	query  = opensearch_set_query(params)

	#header
	n_results = opensearch_get_header(S,query,params)

	a = opensearch_parse_list(S,query,n_results)
	print(a[:,1])
	print(a.shape)

	S.close()
