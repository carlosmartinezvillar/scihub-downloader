import os
import xml.etree.ElementTree as ET
import requests
import numpy as np
import argparse
from tqdm import tqdm
import time
from multiprocessing import Pool
import sys

####################################################################################################
# GLOBAL VARIABLES
####################################################################################################
#Some restrictive parameters
DATA_DIR    = os.getenv('DATA_DIR')
LOGS_DIR    = "./log/"
TEMP_DIR    = DATA_DIR + "temp/"
OS_BASE_URI = "https://scihub.copernicus.eu/dhus/search"    #OpenSearch API service root URI
OD_BASE_URI = "https://scihub.copernicus.eu/dhus/odata/v1/" #OpenData   API service root URI
S2_BAND_IDX = ['B1','B2','B3','B4','B5','B6','B7','B8','B8A','B8A','B9','B10','B11','B12']
S2_BANDS    = [
	'B02_10m','B03_10m','B04_10m','B08_10m','TCI_10m','AOT_10m','WVP_10m','B01_20m','B02_20m',
	'B03_20m','B04_20m','B05_20m','B06_20m','B07_20m','B8A_20m','B11_20m','B12_20m','TCI_20m',
	'AOT_20m','WVP_20m','SCL_20m','B01_60m','B02_60m','B03_60m','B04_60m','B05_60m','B06_60m',
	'B07_60m','B8A_60m','B09_60m','B11_60m','B12_60m','TCI_60m','AOT_60m','WVP_60m','SCL_60m']

#OpenSearch XML Namespaces
NS = {'os':'http://a9.com/-/spec/opensearch/1.1/', 'other':'http://www.w3.org/2005/Atom'}
MTD_NS = {'n1':"https://psd-14.sentinel2.eo.esa.int/PSD/User_Product_Level-2A.xsd",
		  'other':"http://www.w3.org/2001/XMLSchema-instance",
		  'another':"https://psd-14.sentinel2.eo.esa.int/PSD/User_Product_Level-2A.xsd"}

#For storing env locally -- temporary?
USER = None 
PASS = None

#Search Defs
PLATFORMNAME = "Sentinel-2"
PRODUCT      = "S2MSI2A"
START_TIME   = "2022-01-01T00:00:00.000Z"
STOP_TIME    = "2023-06-01T23:59:59:999Z"
RANGE_TIME   = "[%s TO %s]" % (START_TIME,STOP_TIME)
CLOUD_PERCNT = "[0 TO 5]"
BAND_RES     = ["SCL_20m","B02_10m","B03_10m","B04_10m","B08_10m"]


####################################################################################################
# ARGV
####################################################################################################
parser = argparse.ArgumentParser()
parser.add_argument('-f','--input-file',
	help="use a tsv file with results of a previous search instead of a set of coordinates.",
	action='store',
	type=str,
	metavar="<input_file>"
	)
parser.add_argument('-g','--geo_file',
	help='txt file with a list of coordinates to be searched',
	action='store',
	type=str,
	metavar='<geo_file>'
	)

####################################################################################################
# HELPER FUNCTIONS
####################################################################################################
def load_table_and_reduce(path):
	min_area = 50.0
	with open('sites_table.csv') as fp:
		arr = np.array([line.rstrip('\n').split(',') for line in fp.readlines()][1:])
	area = arr[:,2].astype(float)
	large_enough = arr[area > min_area]
	np.savetxt('sites_small_table.csv',large_enough,fmt='%s',delimiter=',',newline='\n')
	np.savetxt('sites_small.txt',large_enough[:,-2:],fmt='%s',delimeter=',',newline='\n')
	print("File saved to sites_small_table.csv and sites_small.txt")


def set_auth_from_env(env_user_str,env_pass_str):
	ENV_USER = os.getenv(env_user_str)
	ENV_PASS = os.getenv(env_pass_str)
	if (ENV_USER is not None):
		if (ENV_PASS is not None):
			global USER
			global PASS
			USER = ENV_USER
			PASS = ENV_PASS
			print("USER and PASS set to env variables.")
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
	print("%i duplicates found." %  n_duplicates) 
	return product_list[index]

####################################################################################################
# OPENSEARCH SEARCH, SET QUERY, PARSE PAGE RESULTS
####################################################################################################
def opensearch_coordinate_list(S,coords_path,params):
	n_results    = 0
	all_products = None

	#LIST OF COORDS
	coords = load_points_from_file(coords_path)

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
			waterpercentage = str(round(float(d.text),6))

		if d.attrib['name'] == 'cloudcoverpercentage':
			cloudpercentage = str(round(float(d.text),6))

	return (uuid,filename,waterpercentage,cloudpercentage)


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
		payload = {'start':current_page*100,'rows':100,'q':query,'orderby':'beginPosition desc'}
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
	Single-function version opensearch_parse_pages() above.
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

####################################################################################################
# DOWNLOADS
####################################################################################################
def odata_image_uri(row,band_res):
	'''
	Build and return the URI for a single SciHub image file. Input row in table follows the format:	
	
		[uuid,filename,waterpercentage,cloudcover,status,datastrip_id,granule_id]

	'''
	# incompatible band-res combos
	assert band_res in S2_BANDS, "odata_image_uri(): Bad band-resolution combination."


	#get uri components from table info in a tidy way
	uuid      = row[0]
	filename  = row[1]
	level     = filename.split('_')[1][3:] #L2A from filename
	tile      = filename.split('_')[-2]
	dstrip    = row[5].split('_')[-2][1:]
	granule   = row[6].split('_')[-3]
	subdir    = "%s_%s_%s_%s" % (level,tile,granule,dstrip)
	ingestion = filename.split('_')[2]
	img_path  = "%s_%s_%s.jp2" % (tile,ingestion,band_res)

	#build the URI and return it
	uri = OD_BASE_URI
	uri += "Products('%s')/"    % uuid
	uri += "Nodes('%s')/"       % filename
	uri += "Nodes('GRANULE')/"
	uri += "Nodes('%s')/"       % subdir
	uri += "Nodes('IMG_DATA')/"
	uri += "Nodes('R%s')/"      % band_res.split('_')[1]
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

	#build and return
	uri = OD_BASE_URI
	uri += "Products('%s')/Nodes('%s')/Nodes('MTD_MSI%s.xml')/$value" % (uuid,filename,level)
	return uri


def odata_get_images_worker(S,safe_folder,uri,thread_id):

	#IMAGE PATH in .SAFE SUBDIR
	subdir   = DATA_DIR + safe_folder + '/'
	img_path = subdir + uri.split('/')[-2].split('(')[1].rstrip(')').strip('\'')

	#DIR CHECK
	if os.path.isfile(img_path):
		if os.path.getsize(img_path) == 0: #FILE SIZE 0?
			os.remove(img_path) #REMOVE FILE
		else:
			print("Found %s. Skipping" % img_path) #FILE GOOD!
			return

	#DOWNLOAD
	res   = S.get(uri,stream=True)
	tsize = int(res.headers.get('content-length',0))
	bsize = 65536
	bar = tqdm(total=tsize,unit='iB',leave=True,unit_scale=True,ncols=80,position=thread_id,
		ascii=True)
	with open(img_path,'wb') as fp:
		for chunk in res.iter_content(bsize):
			bar.update(fp.write(chunk))
	bar.close()
 
	#INCORRECT FILE SIZE
	if  tsize != 0 and bar.n != tsize:
		print("Error. Incorrect file size during download.")
		if os.path.file(img_path):
			os.remove(img_path)


def odata_get_images(S,online):

	for i_r,row in enumerate(online):
		N = online.shape[0]
		#The subir path for all bands in row product
		subdir = DATA_DIR + row[1]+ '/'

		# if not os.path.isdir(subdir):  #DIR PROBLEM -- xml not downloaded	
		# 	print("odata_get_image(): No %s/ subfolder found." % row[1])
		# 	append_tsv_row(DATA_DIR+'error.tsv',row)
		# else: 

		if os.path.isdir(subdir) and os.path.isfile(subdir + 'MTD.xml'):
			print("\n[%i/%i] Downloading bands for product %s" % (i_r,N,row[1]),flush=True)
			pool = Pool(processes=len(BAND_RES))
			for i,b in enumerate(BAND_RES):
				uri = odata_image_uri(row,b)
				pool.apply_async(odata_get_images_worker,args=(S,row[1],uri,i+1))
			pool.close()
			pool.join()
			append_tsv_row(DATA_DIR + 'downloaded.tsv',row) #success


def odata_get_images_error(e):
	print("In odata_get_images_worker() got error: %s" % e)


def odata_get_xmls_worker(S,row,id):
	#row format: [uuid,filename,waterpercentage,cloudcover,status,datastrip_id,granule_id]
	out_path = DATA_DIR + row[1] + "/MTD.xml"

	#IF NOT .SAFE DIR, os.mkdir()
	if not os.path.isdir(DATA_DIR + row[1]):
		os.mkdir(DATA_DIR + row[1])

	#if dir and mtd.xml, do nothing
	if os.path.isfile(out_path):
		if os.path.getsize(out_path) == 0:
			os.remove(out_path)
		else:
			# <--- check file is bad hier?
			print("Non-empty file already in %s" % out_path)
			return True

	#Prepare download
	uri     = odata_mtdxml_uri(row)
	written = 0

	#Download
	try:
		resp = S.get(uri,stream=True)
		total_size = int(resp.headers.get('content-length',0))
		block_size = 2048
		with open(out_path,'wb') as fp:
			for chunk in resp.iter_content(block_size):
				written += fp.write(chunk)
	except: #Unknown err
		print("odata_get_mtdxml_worker(): Error during download.")
		if os.path.isfile(out_path):
			os.remove(out_path)
		os.rmdir(DATA_DIR + row[1])
		return False

	#Incomplete Error
	if total_size != written:
		print("odata_get_mtdxml_worker(): Error. Incorrect file size during download.")
		os.remove(out_path)
		os.rmdir(DATA_DIR + row[1])
		return False

	#Success
	print("[%i] xml file saved to %s" % (id,out_path))
	return True


def odata_get_xmls(S,online):
	N = online.shape[0]
	Z = zip([S]*N, online, range(N))

	start = time.time()
	with Pool(processes=8) as pool:
		result = pool.starmap(odata_get_xmls_worker,Z)
	end = time.time()
	
	result = np.array(result)
	print("odata_get_mtdxml(): time - %s" % (end-start))
	print("%i/%i metadata files downloaded." % (result.sum(),N) )
	return result


def parse_xml(row):
	"""
	Get datastrip and granule id's from the xml metadata file corresponding to row.

	Parameters
	----------
	row : numpy.array
		The row in the array of products.

	Returns
	-------
	datastrip: str
		Extracted datastrip id.

	granule : str
		Extracted granule id.

	"""
	path = DATA_DIR + row[1] + '/MTD.xml'
	print("Parsing %s..." % path)

	# if this throws AssertError something's very wrong...
	assert os.path.isfile(path), "No file found in path %s" % path

	root         = ET.parse(path).getroot()
	gral_info    = root.find('n1:General_Info',MTD_NS)
	product_info = gral_info.find('Product_Info')
	product_char = gral_info.find('Product_Image_Characteristics')
	#INSIDE TAG <Product_Info> -- ALWAYS in XML
	granule_attrib = list(product_info.iter('Granule'))[0].attrib
	datastrip      = granule_attrib['datastripIdentifier']
	granule        = granule_attrib['granuleIdentifier']

	return datastrip, granule


def append_tsv_row(path,row):
	# [uuid,filename,waterpercentage,cloudcover,status,datastrip_id,granule_id]	
	n_cols = len(row)
	fmt = '%s\t'*(n_cols-1) + '%s\n'
	with open(path,'a') as fp:
		fp.write(fmt % tuple(row))


def odata_check_online(S,uri):
	'''
	Takes the given URI and sends request to check if product is currently online or 
	not. S is the requests.Session object. Returns bool.
	'''
	resp = S.get(uri)
	if resp.text == 'true':
		return True
	return False


def get_status_single_thread(S,product_list):
	status_list = []
	N           = product_list.shape[0]

	print("\nChecking Online/Offline status of products...")
	print("="*100)

	#ITERATE THROUGH PRODUCTS AND REQUEST
	for i,row in enumerate(product_list):
		uri  = OD_BASE_URI + "Products('%s')/Online/$value" % row[0] #uuid
		print("[%i/%i]" % (i+1,N),end=' ')
		print(row[1], end=' -- ')

		status = odata_check_online(S,uri) #returns bool

		if status:
			print("online")
		else:
			print("offline") #....set flag here instead? 

		status_list.append(status)
	
	#NumPy for bool ops with whole array
	status_array = np.array(status_list)
	print("\n%i/%i products offline\n" % ((~status_array).sum(), N) )
	return status_array


def get_status_worker(S,idx,N,row):
	uri = OD_BASE_URI + "Products('%s')/Online/$value" % row[0]
	print("[%i/%i]" % (idx+1,N),end=' ')
	print(row[1], end=' -- ')
	resp = S.get(uri)
	if resp.text == 'true':
		print('online')
		status = 'online'
	else:
		print('offline')
		status = 'offline'

	return status


def get_status(S,product_list):
	idxs = [*range(product_list.shape[0])]
	N    = len(idxs)
	Z    = zip([S]*N,idxs,[N]*N,product_list)

	start = time.time()
	with Pool(processes=8) as pool:
		statuses = pool.starmap(get_status_worker,Z,chunksize=1)
	end   = time.time()

	print("get_status(): time - %f" % (end-start) )
	statuses = np.array(statuses)
	if statuses.shape[0] > 0:
		print("\n%s/%s products offline" % ((statuses=='offline').sum(),N))
	return statuses


def trigger_offline_multiple(S,product_list):
	
	for i,row in enumerate(product_list):
		#20 requests max -- 
		if i == 20:
			break

		uuid = row[0]
		uri  = OD_BASE_URI + "Products('%s')/$value" % uuid

		#HTTP header 202 means good
		print("[%i/%i]" % (i+1,product_list.shape[0]),end=" ")
		print("Triggering offline request for %s" % row[1], end=' -- ')
		resp = S.get(uri)
		print("http: %s" % resp.status_code)


def trigger_offline_single(S,row):
	uri = OD_BASE_URI + "Products('%s')/$value" % row[0]
	print("Triggering retrieval of %s -- " % row[0], end='')
	resp = S.get(uri)
	print("http: %s" % resp.status_code)

####################################################################################################
# MAIN
####################################################################################################
if __name__ == '__main__':

	__spec__ = None #TEMP for pdb multithreaded

	params = {
		'coordinates': "",
		'platformname': PLATFORMNAME,
		'producttype': PRODUCT,
		'cloudcoverpercentage': CLOUD_PERCNT,
		'beginPosition': RANGE_TIME,
		'endPosition:': RANGE_TIME,
		'startdate': START_TIME,
		'enddate': STOP_TIME,
		'bands': BAND_RES
	}

	args = parser.parse_args()

	# SET SESSION AUTH
	# ----------------------------------------
	set_auth_from_env('DHUS_USER','DHUS_PASS')
	S      = requests.Session()
	S.auth = (USER,PASS)


	if args.input_file is None:
		# CHECK COORDINATES FILE IS CORRECT
		assert args.geo_file is not None, "In main: args.geo_file is None."
		assert os.path.isfile(args.geo_file), "In main: no %s geo file found." % args.geo_file 

		# I.SEARCH FROM GEOMETRIES, CHECK ON/OFFLINE
		# ----------------------------------------	
		# a. search
		print('\n' + "="*100)
		print("--> SEARCHING FOR PRODUCTS IN %s" % args.geo_file)
		print("="*100)
		results = opensearch_coordinate_list(S,args.geo_file,params)

		# b. Latest status
		print("\nChecking Online/Offline status of products...")
		print("="*100)
		status  = get_status(S,results)
		current = np.append(results,status.reshape((results.shape[0],1)),axis=1)
		online  = current[status=='online']
		offline = current[status=='offline']

		# c. Store online and offline lists
		np.savetxt(DATA_DIR + 'offline.tsv', offline,fmt='%s',delimiter='\t')
		print("List of offline products written to %s" % DATA_DIR+"offline.tsv" )
		np.savetxt(DATA_DIR + 'online.tsv',online,fmt='%s',delimiter='\t')
		print("List of online products to %s" % DATA_DIR+"online.tsv" )

	else:
		# CHECK INPUT FILE IS CORRECT	
		assert os.path.isfile(args.input_file), "%s not found." % args.input_file

		# I.RELOAD PREVIOUS STATE FROM OFFLINE LOG
		# ----------------------------------------
		# a. load		
		print("="*100)
		print("--> RETRIEVING LIST FROM %s" % args.input_file)	
		print("="*100)
		results = np.loadtxt(args.input_file,dtype=str)
		if len(results) == 0:
			print("Empty file loaded. Exiting.")
			sys.exit(0)

		# b. Latest status
		print("\nChecking Online/Offline status of products...")
		print("-"*80)		
		status  = get_status(S,results)
		current = np.append(results[:,0:4],status.reshape((results.shape[0],1)),axis=1)
		online  = current[status=='online']
		offline = current[status=='offline']

		# c. Status feedback + update offline
		updated = ((results[:,-1]=='offline') & (status=='online')).sum()
		print("%i products previously offline now available.\n" % updated)

		# np.savetxt(DATA_DIR + 'offline.tsv', offline,fmt='%s',delimiter='\t')
		# print("Updating offline products in %s" % DATA_DIR+"offline.tsv" )	


	# Online files?
	if len(online) <= 0:
		print("No online products left to download. Exiting.")
		sys.exit(0)


	# II.RETRIEVE METADATA FILES -- ONLINE
	# ----------------------------------------
	print('\n' + "="*100)
	print("RETRIEVING METADATA FILES FOR ONLINE PRODUCTS...")
	print('='*100)
	mtd_down = odata_get_xmls(S,online) #bool numpy.array of downloaded xmls


	# III. PARSE METADATA FILES -- ONLINE
	# ----------------------------------------
	datastrip_col,granule_col = [],[]		
	for i,row in enumerate(online):
		folder = DATA_DIR + row[1] + '/'
		if not os.path.isfile(folder+"MTD.xml"): # NO FILE ERROR
			print("No file %s" % (folder+"MTD.xml"))
			d,g = '-','-'
		else:
			try:
				d,g = parse_xml(row)
			except: #XML PARSE ERROR
				print("%s can't be parsed. Removing it.." % (DATA_DIR+row[1]+'/MTD.xml'))
				os.remove(folder+"MTD.xml")		
				d,g = '-','-'
		datastrip_col.append(d)
		granule_col.append(g)

	online_filed = np.append(online, np.array([datastrip_col,granule_col]).T, axis=1)
	online_clean = online_filed[online_filed[:,-1]!='-']

	# log missing xml's
	for row in online_filed[online_filed[:,-1]=='-']:
		append_tsv_row(DATA_DIR+'error.tsv',row[0:-2])
	errors_file = np.loadtxt(DATA_DIR+'error.tsv',dtype=str)
	np.savetxt(DATA_DIR+'error.tsv',remove_duplicates(errors_file),fmt='%s',delimiter='\t')


	# IV.RETRIEVE IMAGES -- ONLINE
	# ----------------------------------------	
	print('\n' + "="*100)		
	print("RETRIEVING BAND FILES FOR ONLINE PRODUCTS...")
	print('='*100)
	odata_get_images(S,online_clean)

	#remove duplicates in downloaded.tsv file
	downloaded = remove_duplicates(np.loadtxt(DATA_DIR+'downloaded.tsv',dtype=str))
	np.savetxt(DATA_DIR+'downloaded.tsv',downloaded,fmt='%s',delimiter='\t')


	# V.TRIGGER REQUEST FOR SOME (20) PRODUCTS AND EXIT
	# ----------------------------------------
	print('\n' + "="*100)	
	print("TRIGGERING RETRIEVAL OF (UP TO 20) OFFLINE PRODUCTS...")
	print("="*100)
	trigger_offline_multiple(S,offline)

	# VI. REMOVE DONWLOADED FROM OFFLINE TSV
	# ----------------------------------------
	intersect = np.intersect1d(offline[:,0],downloaded[:,0],assume_unique=True,return_indices=True)
	new_offline = np.delete(offline,intersect[1],0)
	np.savetxt(DATA_DIR+'offline.tsv',new_offline,fmt='%s',delimiter='\t')

