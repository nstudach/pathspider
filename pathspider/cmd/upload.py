
import pycurl
import argparse
import logging
import json
import bz2
import dateutil.parser
import os

from io import BytesIO

def register_args(subparsers):
    parser = subparsers.add_parser(name='upload',
                                   help="Uploads data to PTO")

    parser.add_argument("filename", help="Data file in .ndjson")
    parser.add_argument("--campaign", help="Campaign the data belongs to")
    parser.add_argument("--token", help="Authentification token")
    parser.add_argument("--metadata", nargs='+', help="Additional metadata entry", metavar="ENTRY:VALUE")
    parser.add_argument("--geo", action='store_true', help="Adds geolocation to metafile")
    parser.add_argument("--url", default='https://v3.pto.mami-project.eu/raw/', help="URL for PTO data upload")

    # Set the command entry point
    parser.set_defaults(cmd=start_uploader)

def compress_file(filename):
    '''
    compress file to bz2 if not already done
    
    :param filename: filename of file to compress
    :type filename: str
    :return: str -- filename of compressed file
    '''
    if filename.endswith(".bz2"):
        return filename
    else:
        new_filename = filename + ".bz2"
        compressionLevel = 9
        with open(filename, 'rb') as data:
            fh = open(new_filename, "wb")
            fh.write(bz2.compress(data.read(), compressionLevel))
            fh.close()
        return new_filename

def is_duplicate(filename):
    '''
    True if file is in campaign else False
    
    :param filename: filename of file to compress
    :type filename: str
    :return: bool -- True if file is duplicate else False
    '''

    logger = logging.getLogger("uploader")
    url = BASELINK + os.path.basename(filename)
    logger.debug("checking url: " + url)
    answer = check_url(url, ["Authorization: APIKEY " + TOKEN])

    try:
        json.loads(answer)
        logger.info("File already exists")
        return True
    except:
        # we expect "file FILENAME not found" as answer
        if not answer.endswith("not found"):
            logger.debug("Unexpectes answer: " + answer)
        return False

def check_url(url, headers):
    '''
    checks a url with given header options
    returns answer as string
    
    :param url: url for file upload
    :type url: str
    :param header: additional http headers
    :type header: str
    :return: str -- answer from server
    '''
    
    buffer = BytesIO()
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.WRITEDATA, buffer)
    c.setopt(pycurl.HTTPHEADER, headers)
    c.perform()
    c.close()
    return buffer.getvalue().decode('iso-8859-1')

def upload_metadata(filename):
    '''
    Uploads Metadata to PTO
    Updates metadata with PTO added fields
    returns url for measurment data as string
    
    :param filename: filename of metadata
    :type filename: str
    :param file: metadata
    :type file: :class: json
    :return: str -- url for data file or error
    '''
    
    logger = logging.getLogger("uploader")
    link = BASELINK + os.path.basename(filename)
    content_type = ["Content-type: application/json", "Authorization: APIKEY " + TOKEN]
    answer = upload_file (link, content_type, filename)
    logger.debug("uploaded metadata")
    try:
        data = json.loads(answer)
        return (data["__data"])
    except:
        logger.debug("unexpected answer. excepted .json instead: " + answer)
        return "Error"

def upload_data(url, filename, fn_metadata):
    '''
    Uploads datafile to campaign on PTO server
    
    :param url: name of campaign the data belongs to
    :type url: str
    :param filename: filename of file to compress
    :type filename: str
    '''

    logger = logging.getLogger("uploader")
    content_type = ["Content-type: application/bzip2", "Authorization: APIKEY " + TOKEN]
    answer = upload_file(url, content_type, filename)
    logger.debug("uploaded data")
    try:
        data = json.loads(answer)
        logger.info("upload complete")
        save_json(fn_metadata, data)
        logger.info("saved metadata: " + fn_metadata)
    except:
        logger.info("upload failed")
        logger.debug("unexpected answer. excepted .json instead: " + answer)
    
def upload_file(url, headers, filename):
    '''
    Uploads file to campaign on PTO server
    
    :param url: name of campaign the data belongs to
    :type url: str
    :param filename: filename of file to compress
    :type filename: str
    :param headers: additional http headers
    :type headers: str
    :return: str -- answer from server
    '''

    buffer = BytesIO()
    c = pycurl.Curl()
    #set curl options
    # -X PUT
    c.setopt(c.URL, url)
    c.setopt(c.WRITEDATA, buffer)
    # upload the contents of this file --data-binary @file
    c.setopt(c.UPLOAD, 1)
    file = open(filename, "rb")
    c.setopt(c.READDATA, file)
    # -H "Content-type: application/xxx"
    # -H "Authorization: APIKEY $TOKEN"
    c.setopt(pycurl.HTTPHEADER, headers)
    c.perform()
    c.close()
    # File must be kept open while Curl object is using it
    file.close()
    return buffer.getvalue().decode('iso-8859-1')

def save_json(name, data):
    with open(name, mode="w") as mfp:
        json.dump(data, mfp, indent=2)
    
def metadata_from_ps_ndjson(fp):
    '''
    reads data file and extracts meta data
    '''
    
    y = None
    z = None

    for line in fp:
        d = json.loads(line)

        a = dateutil.parser.parse(d['time']['from'])
        b = dateutil.parser.parse(d['time']['to'])

        if y is None or a < y:
            y = a
        
        if z is None or b > z:
            z = b    

    return {'_time_start': y.strftime("%Y-%m-%dT%H:%M:%SZ"),
            '_time_end': z.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "_file_type": "pathspider-v2-ndjson-bz2"}

def write_metadata_for(filename):
    '''
    Creates metafile name and checks if data file is compressed or not
    '''

    metafilename = filename.split(".")[0] + ".meta.json"
    
    if filename.endswith(".bz2"):
        open_fn = bz2.open
    else:
        open_fn = open

    with open_fn(filename) as fp:
        metadata = metadata_from_ps_ndjson(fp)
    
    return metafilename, metadata

def create_metadata(filename, entry, geo):
    '''
    Create and update metadata from a given data file
    
    :param filename: filename of data file
    :type filename: str
    :param entry: Additional meta data tags
    :type entry: list of str
    :param geo: option to add geolocation data
    :type geo: bool
    :return: str -- filename of metadata
    '''

    logger = logging.getLogger("uploader")
    #read out meta data
    logger.debug("extracting metadata from " + filename)
    fn_metadata, metadata = write_metadata_for(filename)

    # add custom entries to metadata
    if entry is not None:
        for element in entry:
            keyword, value = element.split(":")
            metadata[keyword] = value

    # add geolocation
    if geo:
        geolocation = check_url('https://ipinfo.io/' ,[])
        metadata = {**metadata, **json.loads(geolocation)}

    save_json(fn_metadata, metadata)
    logger.info("generated metadata: " + fn_metadata)
    return fn_metadata

def uploader(url, campaign, token, filename, fn_metadata):
    '''
    Uploads a given file to a campaign on the PTO using the provided token.
    Also creates and uploads the neccessary meta data.
    Prevents overwriting existing files on PTO
    
    :param campaign: name of campaign the data belongs to
    :type campaign: str
    :param filename: filename of file to compress and upload
    :type filename: str
    :param token: authentification tocken for PTO API
    :type token: str
    :param entry: Additional meta data tags
    :type entry: list of str
    '''

    logger = logging.getLogger("uploader")
    logger.debug("started uploader")

    global BASELINK
    global TOKEN
    if url.endswith('/'):
        BASELINK = url + campaign + "/"
    else:
        BASELINK = url + "/" + campaign + "/"
    TOKEN = token

    # check if metadata already exist on server
    if not is_duplicate(fn_metadata):

        #upload and read out data link for data upload
        logger.debug("Start processing metadata")
        data_link = upload_metadata(fn_metadata)

        # compress data if necessary and upload data
        logger.debug("Start processing data")
        upload_data(data_link, compress_file(filename), fn_metadata)

def start_uploader(args):
    fn_metadata = create_metadata(args.filename, args.metadata, args.geo)
    uploader(args.url, args.campaign, args.token, args.filename, fn_metadata)



