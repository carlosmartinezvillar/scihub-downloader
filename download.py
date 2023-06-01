import os
import xml.etree.ElementTree as ET
import multiprocessing
import requests
import yaml
import numpy as np
import subprocess
import argparse
from tqdm import tqdm
import time

#Global variables for some restrictive parameters
DATA_DIR    = "dat/"
LOGS_DIR    = "log/"
TEMP_DIR    = DATA_DIR + "temp/"
OS_BASE_URI = "https://scihub.copernicus.eu/dhus/search"    #OpenSearch API service root URI
OD_BASE_URI = "https://scihub.copernicus.eu/dhus/odata/v1/" #OpenData   API service root URI
S2_10_BANDS = ['AOT','B02','B03','B04','B08','TCI','WVP']
S2_20_BANDS = ['AOT','B02','B03','B04','B05','B06','B07','B8A','B11','B12','SCL','TCI','WVP']
S2_60_BANDS = ['AOT','B02','B03','B04','B05','B06','B07','B09','B8A','B11','B12','SCL','TCI','WVP']
S2_BAND_IDS = ['B1','B2','B3','B4','B5','B6','B7','B8','B8A','B8A','B9','B10','B11','B12']

#OpenSearch XML Namespaces
NS = {'os':'http://a9.com/-/spec/opensearch/1.1/', 'other':'http://www.w3.org/2005/Atom'}
MTD_NS = {'n1':"https://psd-14.sentinel2.eo.esa.int/PSD/User_Product_Level-2A.xsd",
		  'other':"http://www.w3.org/2001/XMLSchema-instance",
		  'another':"https://psd-14.sentinel2.eo.esa.int/PSD/User_Product_Level-2A.xsd"}

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
START_TIME   = "2022-05-11T00:00:00.000Z"
STOP_TIME    = "2023-05-11T23:59:59:999Z"
RANGE_TIME   = "[%s TO %s]" % (START_TIME,STOP_TIME)
CLOUD_PERCNT = "[0 TO 10]"
BANDS        = ["B02","B03","B04","B08"] #---> fix to B02_10m, because need SCL_20m not SCL_60m

####################################################################################################
class Downloader():
	def __init__(self):
		self.coord_list = None
		self.parameters = None
		self.query      = None
		self.session    = None

	#set
	#set
	#get
	#get
	#etc
####################################################################################################
def set_auth_from_env(env_user_str,env_pass_str):
	ENV_USER = os.getenv(env_user_str)
	ENV_PASS = os.getenv(env_pass_str)
	if (ENV_USER is not None):
		if (ENV_PASS is not None):
			global USER
			global PASS
			USER = ENV_USER
			PASS = ENV_PASS
			print("USER and PASS set to env variables.\n")
		else:
			print("Env variable given for password is not set.")
	else:
		print("Env variable given for user name is not set.")


def load_points_from_file(path):
	'''
	Load a comma-separated file in path with each line having a <Lat, Lon> format.
	The  lat, lon order is intended to match ESA's OpenSearch format so that each line can be 
	inserted as is in an openSearch query.
	'''
	with open(path,'r') as fp:
		geom_list = [i.rstrip() for i in fp.readlines()]
	return geom_list


def remove_duplicates(product_list):
	'''
	Go through existing numpy array with sentinel product ids and, duh, remove
	duplicates.
	'''
	uuids,index,counts = np.unique(product_list[:,0],return_index=True,return_counts=True)
	n_duplicates = product_list.shape[0] - len(uuids)
	print("%i duplicates removed from original array of %i" %(n_duplicates,product_list.shape[0]))
	return product_list[index]


# PARSE OPENSEARCH PAGE RESULTS
####################################################################################################
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


def opensearch_get_header(S,query,params):

	if len(params['coordinates']) < 80:
		print("Coordinates: %s" % params['coordinates'])
	else:
		print("Coordinates: %s" % params['coordinates'][0:65])
	print("Platform: %s" % params['platformname'],end=', ')
	print("Product: %s" % params['producttype'],end=', ')
	print("Dates: %s/%s" %(params['startdate'][0:10],params['enddate'][0:10]),end=', ')
	print("CloudPct: %s" % params['cloudcoverpercentage'])
	# print(query)

	n_results = 0

	#request
	payload = {'start':0, 'rows':0, 'q':query} #return 0 results
	res     = S.get(OS_BASE_URI,params=payload)

	#Correct response codes
	assert res.status_code != 401, "opensearch_get_header(): Got HTTP 401: Check user and password."
	assert res.status_code == 200, "opensearch_get_header(): Got HTTP %s." % res.status_code
	
	#Parse response
	root      = ET.fromstring(res.text)
	n_results = int(root.find('os:totalResults',namespaces=NS).text)
	n_pages   = n_results//100 + 1

	#Print feedback and return
	print("Found %i results in %i page(s)." % (n_results,n_pages))
	return n_results


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


def opensearch_parse_pages(S,query,params):
	'''
	Parse all pages returned by a OpenSearch query.
	Returns a numpy array with the products' information.
	'''
	n_results = opensearch_get_header(S,query,params)
	n_pages = n_results//100 + 1

	print("Parsing pages...\n")

	for current_page in range(n_pages):
		payload = {'start':current_page*100,'rows':100,'q':query}
		resp    = S.get(OS_BASE_URI,params=payload)	
		root    = ET.fromstring(resp.text)
		page_results = opensearch_parse_page(root)

		if current_page == 0:
			results = page_results
		else:
			results = np.concatenate([results,page_results],axis=0)

	return results


def opensearch_parse(S,query,params):
	'''
	Iterate through all pages returned by OpenSearch query and parse them.
	Returns a numpy array with the products' information.
	'''

	#FIND NR OF PAGES TO PARSE
	n_results = opensearch_get_header(S,query,params)
	n_pages   = n_results//100 + 1 
	# remainder = n_results % 100  #requesting 100 still returns what's left at the end
	entries   = []

	#ITERATE THRU PAGES BY 100 RESULTS AT A TIME
	print("Parsing results...")

	for current_page in range(n_pages):
		#GET PAGE -- SERVER REQUESTs
		payload = {'start':current_page*100,'rows':100,'q':query}
		resp = S.get(OS_BASE_URI,params=payload)
		root = ET.fromstring(resp.text)

		#PARSE XML-- list of entries
		entries_xml = root.findall('other:entry',namespaces=NS)
		
		#PARSE XML-- each entry in page
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

			#APPEND ENTRIES TO LIST
			entries += [(uuid,filename,waterpercentage,cloudcoverpercentage)]

	return np.array(entries)


# ODATA METADATA AND IMAGES
####################################################################################################
def odata_image_uri(row,band,resolution):
	'''
	Build and return the URI for a single SciHub image file. Input row in table follows the format:	
	
		[uuid,filename,waterpercentage,cloudcover,status,datastrip_id,granule_id]

	'''
	## do a check for incompatible band-res combos (global vars) -------------------------------TODO


	#get uri components from table info in a tidy way
	uuid      = row[0]
	filename  = row[1]
	level     = filename.split('_')[1][3:] #L2A from filename
	tile      = filename.split('_')[-2]
	dstrip    = row[5].split('_')[-2][1:]
	granule   = row[6].split('_')[-3]
	subdir    = "%s_%s_%s_%s" % (level,tile,granule,dstrip)
	ingestion = filename.split('_')[2]
	img_path  = "%s_%s_%s_%s.jp2" % (tile,ingestion,band,resolution)

	#build the URI and return it
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
	Build and set the URI for the metadata file of a product
	'''
	#split/get URI elements for table of metadata
	uuid     = row[0]
	filename = row[1]
	level    = filename.split('_')[1][3:]
	# tile     = filename.split('_')[-2]

	#build and return
	uri = OD_BASE_URI
	uri += "Products('%s')/" % uuid
	uri += "Nodes('%s')/" % filename
	uri += "Nodes('MTD_MSI%s.xml')/$value" % level 
	return uri


def odata_get_image(S,uri):
	res   = S.get(uri,stream=True)
	tsize = int(res.headers.get('content-length',0))
	bsize = 1024
	# opath = DATA_DIR + row[1]

	#Check for metadata
	subdir = DATA_DIR + row[1]
	if not os.path.isdir(subdir):
		print("No %s subdirectory found." % subdir)
		return

	out_path = 

	bar = tqdm(total=tsize, unit='iB', unit_scale=True)
	with open(out_path,'wb') as fp:	
		for chunk in res.iter_content(bsize):
			bar.update(fp.write(chunk))
	bar.close()

	if  tsize != 0 and bar.n != tsize:
		print("Error. Incorrect file size during download.")
		os.remove(out_file_path)

def odata_get_mtdxml(S,row):
	'''
	Assuming this format:
		[uuid,filename,waterpercentage,cloudcover,status,datastrip_id,granule_id]	
	'''
	out_file_path = "%s%s/MTD.xml" % (DATA_DIR, row[1])

	#if dir and mtd.xml --> do nothing
	if os.path.isdir(DATA_DIR+row[1]) and os.path.isfile(out_file_path):
		print("MTD.xml file found in %s/ subdir" % row[1])
		return

	#Make dir with SciHub .SAFE file name
	if not os.path.isdir(DATA_DIR + row[1]):
		os.mkdir(DATA_DIR + row[1])

	uri  = odata_mtdxml_uri(row)

	#Download
	resp = S.get(uri,stream=True)
	total_size = int(resp.headers.get('content-length',0))
	block_size = 1024
	bar = tqdm(total=total_size, unit='iB',unit_scale=True)
	with open(out_file_path,'wb') as fp:
		for chunk in resp.iter_content(block_size):
			written = fp.write(chunk)
			bar.update(written)
	bar.close()

	#Error
	if total_size != 0 and bar.n != total_size:
		print("Error. Incorrect file size during download.")

		os.rmdir(DATA_DIR + row[1])

	print("Metadata file saved to %s" % out_file_path)


def parse_mtdxml(row):
	'''
	Get datastrip id and granule id from MTD_MSI
	'''
	path = './dat/' + row[1] + '/MTD.xml'

	# path chheck here before everything breaks in the next line
	assert os.path.isfile(path), "No file found in path %s" % path

	root         = ET.parse(path).getroot()
	gral_info    = root.find('n1:General_Info',MTD_NS)
	product_info = gral_info.find('Product_Info')
	product_char = gral_info.find('Product_Image_Characteristics')

	#INSIDE TAG <Product_Info> -- ALWAYS in XML
	granule_attrib = list(product_info.iter('Granule'))[0].attrib
	datastrip      = granule_attrib['datastripIdentifier']
	granule        = granule_attrib['granuleIdentifier']

	#INSIDE TAG <Product_Image_Characterstics> -- Nevermind
	# "For a given DN in [1;2^15-1], the L2A BOA reflectance value will be: 
	#	L2A_BOAi = (L2A_DNi + BOA_ADD_OFFSETi) / QUANTIFICATION_VALUEi "
	# char.find('QUANTIFICATION_VALUES_LIST').find('BOA_QUANTIFICATION_VALUE').text
	# quantval = list(product_char.iter('BOA_QUANTIFICATION_VALUE'))[0].text
	# offsets  = product_char.find('BOA_ADD_OFFSET_VALUES_LIST')
	# new_dict = {}
	# if offsets is not None:
	# 	for e in offsets:
	# 		d = e.attrib
	# 		d['']
	# offsets  = [e for e in product_char.find('BOA_ADD_OFFSET_VALUES_LIST').iter()]
	return datastrip, granule

# DEAL W/ PRODUCT STATUS
####################################################################################################
def odata_check_online(S,uri):
	'''
	Takes the given URI and sends request to check if product is currently online or 
	not. S is the requests.Session object. Returns bool.
	'''
	resp = S.get(uri)
	if resp.text == 'true':
		return True
	return False


def odata_trigger_offline_request(S,product_list):
	for i,row in enumerate(product_list):
		if i == 20: # -------------------------------------------------------------------------> FIX
			break
		uuid = row[0]
		uri  = OD_BASE_URI + "Products('%s')/$value" % uuid

		#HTTP header 202 means good
		print("[%i/%i]" % (i,len(uuid)) ,end=" ")
		print("Triggering offline request for product %s" % uuid, end=' -- ')
		resp = S.get(uri)
		print("http: %s" % resp.status_code)


def odata_get_status(S,product_list):
	status_list = []
	N           = product_list.shape[0]

	print("\nChecking Online/Offline status of products...")
	print("="*100)

	#ITERATE THROUGH PRODUCTS AND REQUEST
	for i,row in enumerate(product_list):
		uuid = row[0]
		uri  = OD_BASE_URI + "Products('%s')/Online/$value" % uuid
		print("[%i/%i]" % (i+1,N),end=' ')
		print(uuid, end=' -- ')
		status = odata_check_online(S,uri) #returns bool

		if status:
			print("online")
		else:
			print("offline") #....set flag here instead? 

		status_list.append(status)
	
	#NumPy for bool ops in whole array
	status_array = np.array(status_list)
	print("\n%i/%i products offline\n" % ((~status_array).sum(), N) )

	#Whole array is string so set new column as str flag
	product_list = np.append(product_list, np.array(['?']*N).reshape((N,1)),axis=1)
	product_list[status_array,-1]  = 'online'
	product_list[~status_array,-1] = 'offline'
	
	#Save array into .tsv file for later re-call
	print("Writing list of offline products to %s." % DATA_DIR + "offline.txt" )
	np.savetxt(DATA_DIR + 'offline.txt', product_list[~status_array],fmt='%s',delimiter='\t')
	print("Writing list of online products to %s." % DATA_DIR + "online.txt" )
	np.savetxt(DATA_DIR + 'online.txt', product_list[status_array],fmt='%s',delimiter='\t')
	return product_list

# LOAD MULTIPLE GEOMETRIES
####################################################################################################
def opensearch_coordinate_list(S,coords_path,params):
	n_results    = 0
	all_products = None

	#LIST OF COORDS
	coords = load_points_from_file(coords_path)[0:2] #--------------------------------------> REMOVE

	for i,c in enumerate(coords):
		#UPDATE COORDS IN QUERY
		params['coordinates'] = c
		query     = opensearch_set_query(params)

		#PARSE PAGES OF RESULTS
		if i == 0:
			all_products = opensearch_parse_pages(S,query,params)
			if len(all_products.shape) == 1:
				all_products = all_products.reshape((0,4))
		else:
			products = opensearch_parse_pages(S,query,params)
			if len(products) > 0:
				all_products = np.concatenate([all_products,products],axis=0)


	n_results = all_products.shape[0]
	print('-'*80)
	print("Found %i products for %i geometries." % (n_results,len(coords)))
	clean_products = remove_duplicates(all_products)
	print('-'*80)

	return clean_products

####################################################################################################
# MAIN
####################################################################################################
if __name__ == '__main__':

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

	#SET SESSION AUTH
	set_auth_from_env('DHUS_USER','DHUS_PASS')
	S      = requests.Session()
	S.auth = (USER,PASS)

	#Search from geometries, check status, separate online v offline
	arr = opensearch_coordinate_list(S,'test_sites.txt', params)
	arr = odata_get_status(S, arr)
	online = arr[arr[:,4]=='online'][0:3] #-------------------------------------------------->REMOVE

	#Retrieve metadata files
	print("RETRIEVING METADATA FILES FOR ONLINE PRODUCTS...")
	print('='*100)

	datastrip_col, granule_col = [],[]
	
	for row in online:
		odata_get_mtdxml(S,row)
		datastrip, granule = parse_mtdxml(row)
		datastrip_col.append(datastrip)
		granule_col.append(granule)
	online = np.append(online, np.array([datastrip_col,granule_col]).T, axis=1)
	# online = np.append(online, np.column_stack((datastrip_col,granule_col)),axis=1)

	pass

	#Retrieve images -- Want B02_10m, B03_10m, B04_10m, B08_10m, SCL_20m
	print("RETRIEVING METADATA FILES FOR ONLINE PRODUCTS...")
	print('='*100)
	for row in online:
		for band in BANDS: #--ugly
			uri = odata_image_uri(row,band,'10m')
			print(uri)
			# odata_get_image(S,uri)




