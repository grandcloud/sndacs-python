'''
Created on 2011-7-27

'''

from hashlib import md5
from urlparse import urlparse
from sndacspylib.snda_cs_config import Config
from sndacspylib.snda_cs_mime import MIME_TYPES as Types
from sndacspylib.snda_cs_exception import *
from sndacspylib.snda_cs_model import *

import base64
import cs_rest as CS
import datetime
import httplib
import logging
import os
import sndacspylib.snda_cs_genutilities as Util
import string
import time
import xml.sax


_NUMBER_OF_RETRIES_ = 3

_MD_SIZE_               = 'size'
_MD_CREATE_TIME_        = 'create-time'
_MD_LAST_MODIFIED_TIME_ = 'last-modified-time'

appLog = logging.getLogger('app')
cbackLog = logging.getLogger('app.cback')
errLog = logging.getLogger('err')

IMPORT_FLAG = False
try:
    import rfc3339
    IMPORT_FLAG = True
except ImportError, e:
    IMPORT_FLAG = False

def _get_file_size_(file_pointer):
    """
    Get file size
    """
    #get current position
    cur_pos = file_pointer.tell()
    
    #seek the end of file
    file_pointer.seek(0, os.SEEK_END)
    
    #get file pointer position, actual the file size
    file_size = file_pointer.tell()
    
    #recover the file pointer original position
    file_pointer.seek(cur_pos, os.SEEK_SET)
    
    return file_size

def _default_cb_( method, bucket, key, length):
    cbackLog.info( '%s %s %s %s' % (method, bucket, key, length) )

class _CSObject_:
    
    def __init__(self, data, metadata={}, size=0, last_modified=0):
        self.data = data
        self.metadata = metadata
        self.size = size
        self.last_modified = last_modified
        
class _Response_:
    
    def __init__(self, http_response):
        self.body = ''
        self.http_response = http_response
        
        if http_response.status < httplib.MULTIPLE_CHOICES:
            self.body = http_response.read()
            
    def _get_header_(self, header):
        """
        Get http header
        
        @type header: string
        @param header: get http header value which name is [header]
        
        @rtype: string
        @return: value of header
        """
        return self.http_response.getheader(header)
    
    def _get_msg_(self):
        """
        Get message of HTTPResponse object
        
        @rtype: string
        @return: get http response message
        """
        return self.http_response.msg
    
    def _get_headers_(self):
        """
        Get all http response headers
        
        @rtype: dictionary
        @return: http response headers
        """
        return self.http_response.getheaders
    
    def _get_status_(self):
        """
        Get status of HTTPResponse object
        
        @rtype: number
        @return: http response status
        """
        return self.http_response.status
    
    def _get_reason_(self):
        """
        Get reason of HTTPResponse object
        
        @rtype: string
        @return: http response reason
        """
        return self.http_response.reason
    
    def _get_version_(self):
        """
        Get version of HTTPResponse object
        
        @rtype: string
        @return: http version
        """
        return self.http_response.version
    
    def _get_date_(self):
        """
        Get date header of HTTPResponse object
        
        @rtype: string
        @return: http response date header value
        """
        return self._get_header_('Date')
    
    def _get_last_modified_(self):
        """
        Get Last-Modified header of HTTPResponse object
        
        @rtype: string
        @return: http response last-modified header value
        """
        return self._get_header_('Last-Modified')
    
    def _get_content_type_(self):
        """
        Get Content-Type header of HTTPResponse object
        
        @rtype: string
        @return: http response content-type header value
        """
        return self._get_header_('Content-Type')
    
    def _get_content_length_(self):
        """
        Get Content-Length header of HTTPResponse object
        
        @rtype: string
        @return: http response content-length header value
        """
        return self._get_header_('Content-Length')
    
    def _get_etag_ (self):
        """
        Get ETag header of HTTPResponse object with no double-quote
        
        @rtype: string
        @return: http response etag header value which trimmed double-quote('"')
        """
        return (self._get_header_('etag').strip('\"') )
    
    def _get_etag_with_quote_(self):
        """
        Get ETag header of HTTPResponse object
        
        @rtype: string
        @return: http response etag header value
        """
        return self._get_header_('etag')
    
    def _get_content_md5_(self):
        """
        Get Content-MD5 header of HTTPResponse object
        
        @rtype: string
        @return: http response content-md5 header value
        """
        return self._get_header_('Content-MD5')
    
    def _get_server_(self):
        """
        Get Server header of HTTPResponse object
        
        @rtype: string
        @return: http response server header value
        """
        return self._get_header_('Server')
    
class _GETResponse_(_Response_):
    
    def __init__(self, http_response):
        _Response_.__init__(self, http_response)
        response_headers = http_response.getheaders()
        metadata = self._get_snda_metadata_(response_headers)
        size = self._get_content_length_()
        last_modified = self._get_last_modified_()
        self.object = _CSObject_(self.body, metadata, size, last_modified)
        
    def _get_snda_metadata_(self, headers):
        """
        Get HTTPReponse object headers which has "x-snda-meta-" prefix
        
        @type headers: dictionary
        @param headers: http response headers to get meta data from
        
        @rtype: dictionary
        @return: meta data
        """
        metadata = {}
#        for header_key in headers.keys():
#            if header_key.lower().startswith(CS.METADATA_PREFIX):
#                metadata[header_key] = headers[header_key]
#                del headers[header_key]
        
        # header is a tuple, like (header, value)
        for header in headers:
            if header[0].lower().startswith(CS.METADATA_PREFIX):
                metadata[header[0]] = header[1]
                
        return metadata
    
    def _get_metadata_value_(self, key):
        """
        Get http header which is metadata
        
        @type key: string
        @param key: key of meta data
        
        @rtype: string
        @return: value of meta data
        """
        if self.object.metadata.has_key(key):
            return self.object.metadata[key]
        else:
            raise InvalidAttribute(key)
        
    def _get_data_(self):
        return self.object.data
    
    
class SNDA_CS:
    """
    This class encapsulates operations on SNDA ECS buckets.
    """
    def __init__(self, ConnectionObject):
        """
        ConnectionObject: An instance of ConnectionObject used to communicate with ECS
        
        @type ConnectionObject: SNDAAuthConnection
        @param ConnectionObject: authentication connection with snda grandcloud storage
        """
        self.CONN = ConnectionObject
        self.ListOfBuckets = {}
        
    class _ListOfAllBucketsResponse_(_Response_):
        
        def __init__(self, http_response):
            _Response_.__init__(self, http_response)
            if (self._get_status_() < httplib.MULTIPLE_CHOICES):
                handler = self._ListOfAllBucketsHandler_()
                xml.sax.parseString(self.body, handler)
                self.entries = handler.entries
            else:
                self.entries = []
            
        class _ListOfAllBucketsHandler_(xml.sax.ContentHandler):
            """
            SAX ContentHandler to parse list buckets xml docs.
            """
            def __init__(self):
                """
                _ListOfAllBucketsHandler_ constructor
                """
                self.entries = []
                self.curr_entry = None
                self.curr_text = ''
                
            def startElement(self, name, attrs):
                if name == 'Bucket':
                    self.curr_entry = self.Bucket()

            def endElement(self, name):
                if name == 'Name':
                    self.curr_entry.name = self.curr_text
                elif name == 'CreationDate':
                    self.curr_entry.creation_date = self.curr_text
                elif name == 'Bucket':
                    self.entries.append(self.curr_entry)
                elif name == 'Location':
                    self.curr_entry.location = self.curr_text

            def characters(self, content):
                self.curr_text = content
                
            class Bucket:
                def __init__(self, name='', creation_date='', location='huadong-1'):
                    self.name = name
                    self.creation_date = creation_date
                    self.location = location
                    
    def get_bucket_name(self, bucketName):
        """
        Get bucket name from bucket lists
        """
        if self.ListOfBuckets.has_key(bucketName):
            return self.ListOfBuckets[bucketName]
        else:
            return None
    
    def get_list_of_buckets(self):
        """
        Retrieve the list of all buckets owned by the credentials used to construct the ConnectionObject
        contained herein
        """
        try:
            self.ListOfBuckets = {}
            resp = self._ListOfAllBucketsResponse_( self.CONN.make_request ( method = 'GET' ) )

        except Exception, e:
            logging.error ('ERROR %s' % e)
            raise e
        
        try:
            import rfc3339
            for entry in resp.entries:
                entry.creation_date = rfc3339.parse_datetime(entry.creation_date)
                self.ListOfBuckets[entry.name] = entry
        except ImportError, e:
            for entry in resp.entries:
                entry.creation_date = datetime.datetime.strptime(entry.creation_date.split('+')[0],
                                                                 "%Y-%m-%dT%H:%M:%S.%f")
                self.ListOfBuckets[entry.name] = entry

        return self.ListOfBuckets
    
    def add_bucket(self, bucketName, location="huabei-1"):
        """
        Add a new bucket with the name bucketName
        
        @type bucketName: string
        @param bucketName: bucket name
        """
        try:
            location_xml = Util.generate_bucket_location_xml(location)
            headers = {'Content-Length' : len(location_xml)}
            resp = _Response_(self.CONN.make_request(method = 'PUT', bucket = bucketName, headers = headers, data = location_xml))
        except Exception, e:
            errLog.error('ERROR %s' % e)
            raise e
        
        return
    
    def delete_bucket(self, bucketName):
        """
        Delete specified bucket, fails if bucket has any keys
        
        @type bucketName: string
        @param bucketName: bucket name
        """
        try:
            resp = _Response_(self.CONN.make_request(method = 'DELETE', bucket = bucketName))
        except Exception, e:
            errLog.error('ERROR %s' % e)
            raise e
        
    def delete_bucket_recursive(self, bucketName):
        """
        Recursively delete all keys in the bucket and then deletes the bucket.
        
        @type bucketName: string
        @param bucketName: bucket name
        """
        try:
            BucketContent = SNDA_Bucket(self.CONN, bucketName)
            BucketContent.get_list_of_keys_in_bucket()
            for entry in BucketContent.bucketContent:
                key = SNDA_Object(self.CONN, bucketName, entry['name'])
                key.delete_object()
                #really need a little sleep
                time.sleep(0.00)
            
            self.delete_bucket(bucketName)                
            
        except Exception, e:
            errLog.error('ERROR %s' % e)
            raise e
            
        return
        

def _upload_dir_cb_(bucketName, dirname, names):
    conn = CS.SNDAAuthConnection(Config.CSProperties['AccessKey'], \
                                       Config.CSProperties['SecretKey'], ( Config.CSProperties['SecureComm'] == True))
    
    num_files_uploaded = 0
    
    for entry in names:
        full_path = os.path.join(dirname, entry)
        if os.path.isfile(full_path):
            (drive, keyName) = os.path.splitdrive(full_path)
            keyName = os.path.normcase(keyName)
            keyName = os.path.normpath(keyName)
            if (keyName[0] == os.sep):
                keyName = keyName[1:]
                
            #convert file name to 'home|opt|cache|userinfo_cache.data' form
            keyName = string.replace(keyName, os.sep, Util.DELIMITER)
                        
            key = SNDA_Object( conn, bucketName, keyName )
            num_files_uploaded +=1
            key.put_object_from_file ( full_path )
            appLog.debug( '[%d] Processing [%s]: Uploaded <%s>' % (num_files_uploaded, dirname, entry) )
            
            time.sleep(0.00)
    
    appLog.info ('Uploaded [%d] files in <%s> to bucket=%s' % (num_files_uploaded, dirname, bucketName) )
    
    return


def _download_dir_cb_(parent_dir, child_dir, child_file, arg):
    conn = CS.SNDAAuthConnection(Config.CSProperties['AccessKey'], \
                                       Config.CSProperties['SecretKey'], ( Config.CSProperties['SecureComm'] == True))
    
    num_files = 0
    for entry in child_dir:
        entry = string.replace ( entry, Util.DELIMITER, os.sep)
        dir_path = arg[1]+os.sep+entry
        if ( os.path.exists (dir_path) == False):
            os.mkdir ( dir_path)
            appLog.debug ('Created <DIR> %s' % dir_path)
            
    for entry in child_file:
        file_name = string.replace ( entry, Util.DELIMITER, os.sep)        
        file_path = arg[1]+os.sep+file_name
        key = SNDA_Object ( conn, arg[0], entry)
        fDownloaded = key.sync_download_to_file ( file_path)
        if (fDownloaded):
            appLog.debug ('Downloded %s' % entry )
            num_files += 1
        
    appLog.info ( '<%s>: Downloaded %d files' % (parent_dir, num_files) )
    
    return 


class SNDA_Bucket:
    """
    This class represents a bucket and the operations to get the contents of this bucket
    """
    
    def __init__(self, ConnectionObject, bucketName, cb = _default_cb_):
        """
        SNDA_Bucket constructor
        
        @type ConnectionObject: SNDAAuthConnection
        @param ConnectionObject: authentication connection with snda grandcloud storage
        @type bucketName: string
        @param bucketName: bucket name
        @type cb: function
        @param cb: callback function
        """
        self.CONN = ConnectionObject
        self.bucketName = bucketName
        self.cb         = cb
        self._reinit_()
        
    def _reinit_(self):
        """
        re-initialize the SNDA_Bucket object
        """
        self.bucketContent = []        
        self.root_dir= Util.my_dir( access_key=u'', name=u'',depth=0)
        self.dir_stack = []
        self.dir_stack.append(self.root_dir)
        self.count = 0
        self.keyName = ''
    
    class _ListBucketResponse_(_Response_):
        def __init__(self, http_response):
            _Response_.__init__(self, http_response)
            if self._get_status_() < httplib.MULTIPLE_CHOICES:
                handler = self._ListBucketHandler_()
                xml.sax.parseString(self.body, handler)
                self.entries = handler.entries
                self.common_prefixes = handler.common_prefixes
                self.name = handler.name
                self.marker = handler.marker
                self.prefix = handler.prefix
                self.is_truncated = handler.is_truncated
                self.delimiter = handler.delimiter
                self.max_keys = handler.max_keys
                self.next_marker = handler.next_marker
            else:
                self.entries = []
        
        class _ListBucketHandler_(xml.sax.ContentHandler):
            def __init__(self):
                self.entries = []
                self.curr_entry = None
                self.curr_text = ''
                self.common_prefixes = []
                self.curr_common_prefix = None
                self.name = ''
                self.marker = ''
                self.prefix = ''
                self.is_truncated = False
                self.delimiter = ''
                self.max_keys = 0
                self.next_marker = ''
                self.is_echoed_prefix_set = False
                
            class Owner:
                def __init__(self, id='', display_name=''):
                    self.id = id
                    self.display_name = display_name

            class ListEntry:
                def __init__(self, key='', last_modified=None, etag='', size=0, storage_class='', owner=None):
                    self.key = key
                    self.last_modified = last_modified
                    self.etag = etag
                    self.size = size
                    self.storage_class = storage_class
                    self.owner = owner

            class CommonPrefixEntry:
                def __init(self, prefix=''):
                    self.prefix = prefix

            def startElement(self, name, attrs):
                if name == 'Contents':
                    self.curr_entry = self.ListEntry()
                elif name == 'Owner':
                    self.curr_entry.owner = self.Owner()
                elif name == 'CommonPrefixes':
                    self.curr_common_prefix = self.CommonPrefixEntry()
                    self.is_echoed_prefix_set = True

            def endElement(self, name):
                if name == 'Contents':
                    self.entries.append(self.curr_entry)
                elif name == 'CommonPrefixes':
                    self.common_prefixes.append(self.curr_common_prefix)
                    self.is_echoed_prefix_set = False
                elif name == 'Key':
                    self.curr_entry.key = self.curr_text
                elif name == 'LastModified':
                    if IMPORT_FLAG:
                        self.curr_entry.last_modified = rfc3339.parse_datetime(self.curr_text)
                    else:
                        self.curr_entry.last_modified = datetime.datetime.strptime(self.curr_text.split('+')[0],
                                                                 "%Y-%m-%dT%H:%M:%S.%f")
                elif name == 'ETag':
                    self.curr_entry.etag = self.curr_text
                elif name == 'Size':
                    self.curr_entry.size = int(self.curr_text)
                elif name == 'ID':
                    self.curr_entry.owner.id = self.curr_text
                elif name == 'DisplayName':
                    self.curr_entry.owner.display_name = self.curr_text
                elif name == 'StorageClass':
                    self.curr_entry.storage_class = self.curr_text
                elif name == 'Name':
                    self.name = self.curr_text
                elif name == 'Prefix' and self.is_echoed_prefix_set:
                    self.curr_common_prefix.prefix = self.curr_text
#                elif name == 'Prefix' and self.is_echoed_prefix_set:
#                    self.curr_common_prefix.prefix = self.curr_text
#                elif name == 'Prefix':
#                    self.prefix = self.curr_text
#                    self.is_echoed_prefix_set = True
                elif name == 'Marker':
                    self.marker = self.curr_text
                elif name == 'IsTruncated':
                    self.is_truncated = self.curr_text == 'true'
                elif name == 'Delimiter':
                    self.delimiter = self.curr_text
                elif name == 'MaxKeys':
                    self.max_keys = int(self.curr_text)
                elif name == 'NextMarker':
                    self.next_marker = self.curr_text
#                self.curr_text = ''

            def characters(self, content):
#                self.curr_text += content
                self.curr_text = content
                
    def _walk_dir_(self, start, cb_method, arg):
        """
        Walks an in-memory directory stucture starting at "start" and invokes the
        passed in cb_method on each child in the directory and passes along to the
        cb_method the arguments passed in "arg"
        This is a recursive function
        """
        
        parent_dir = start.access_key        
        child_dir = []
        child_file = []
        
        keys = start.children.keys()
        for k in keys:
            entry = start.get_child( k )
            if ( entry.__name__ == 'my_dir'):
                child_dir.append ( entry.access_key )
            elif (entry.__name__ == 'my_file'):
                child_file.append (entry.access_key )
            else:
                raise Failed()

        cb_method ( parent_dir, child_dir, child_file, arg )
        
        for k in keys:
            entry = start.get_child(k)
            if ( entry.__name__ == 'my_dir'):
                self._walk_dir_ ( entry, cb_method, arg)
                        
        return
    
    def get_list_of_keys_in_bucket ( self, prefixDir=u'', delimiter=Util.DELIMITER):
        """
        This function does handles pagination.  Get's all keys in the bucket identified by 
        self.bucketName, 1000 at a time and store them in self.bucketContent
        
        @type prefixDir: string
        @param prefixDir: specified prefix that keys have while listing from bucket
        @type delimiter: string
        @param delimiter: specified delimiter to organize fs tree
        """
        marker = ''
        query_args = {}
        while True:
            try:
                if marker is not None and marker != '':
                    query_args['marker'] = marker
                if delimiter is not None and delimiter != '':
                    query_args['delimiter'] = delimiter
                if prefixDir is not None and prefixDir != '':
                    query_args['prefix'] = prefixDir
                resp = self.CONN.make_request ( method='GET', bucket=self.bucketName, \
                                                query_args=query_args )
            except Exception, e:
                errLog.error ('ERROR %s' % e)
                raise e 

            keylist = self._ListBucketResponse_( resp )
            
            if self.cb:
                self.cb('ALLKEYS', self.bucketName, '', self.count )
                
            for entry in keylist.entries:
                if entry and entry.owner:
                    display_name = entry.owner.display_name
                else:
                    display_name = None
                
                self.bucketContent.append( {'index': self.count, 'type': 'file', 'name':entry.key, 
                                            'size':entry.size, 'owner':display_name, \
                                            'last_modified':entry.last_modified, 'etag':entry.etag} )
                self.count = self.count + 1

            if (keylist.is_truncated == False):
                # we are done...
                break
            if marker == entry.key:
                break
            else:
                marker = entry.key

        return self.bucketContent
    
    def get_keys_in_bucket_as_fstree(self, prefixDir=u'', delimiter=Util.DELIMITER):
        """
        List all keys in bucket in a shape of tree in file system
        
        @type prefixDir: string
        @param prefixDir: specified prefix that keys have while listing from bucket
        @type delimiter: string
        @param delimiter: specified delimiter to organize fs tree
        """
        query_args = {}
        try:
            if prefixDir is not None and prefixDir != '':
                query_args['prefix'] = prefixDir
            if delimiter is not None and delimiter != '':
                query_args['delimiter'] = delimiter
            resp = self.CONN.make_request ( method='GET', bucket=self.bucketName, \
                                            query_args=query_args )
        except CSError, e:
            logging.error ( str(e) )
            raise e
        except Exception, f:
            logging.error ('ERROR %s' % f)
            raise f
        else:

            dirContents = self._ListBucketResponse_( resp )

            if self.cb:
                self.cb('KEYLIST', self.bucketName, prefixDir, self.count )

            topdir = self.dir_stack.pop()

            for fileEntry in dirContents.entries:
                full_name = fileEntry.key
                j = full_name.rfind(Util.DELIMITER)
                if j == len(full_name) - 1:
                    continue
                file_name = full_name[j+1:]
                if fileEntry and fileEntry.owner:
                    display_name = fileEntry.owner.display_name
                else:
                    display_name = None
                f = Util.my_file( fileEntry.key, file_name, topdir.depth+1, fileEntry.size, \
                                  display_name, fileEntry.last_modified, fileEntry.etag, prefixDir )

                topdir.add_child ( f )
                self.bucketContent.append(f)
                self.count = self.count + 1

            for dirEntry in dirContents.common_prefixes:
                if not hasattr(dirEntry, 'prefix'):
                    continue
                full_name = dirEntry.prefix[:-1]
                j = full_name.rfind(Util.DELIMITER)
                
                dir_name = full_name[j+1:]
                
                d = Util.my_dir ( full_name, dir_name, topdir.depth+1, prefixDir)
                
                topdir.add_child ( d )
                self.dir_stack.append(d)
                self.count = self.count + 1
                
#            for dirEntry in dirContents.common_prefixes:
#                if not hasattr(dirEntry, 'prefix'):
#                    continue
#                full_name = dirEntry.prefix[:-1]
#                j = full_name.rfind(Util.DELIMITER)
#                
#                dir_name = full_name[j+1:]                    
#                d = topdir.get_child( dir_name)
#                if not d:
#                    raise Failed()
#
#                self.dir_stack.append(d)
#                self.get_keys_in_bucket_as_fstree( dirEntry.prefix, delimiter )

            return (self.bucketContent, self.dir_stack)
    
    def upload_dir(self, root_dir):
        """
        Uploads all files under root_dir up to the bucket identified by self.bucketName recursively
        
        @type root_dir: string
        @param root_dir: folder path to be upload
        """
        
        if root_dir is '':
            raise InvalidAttribute('root_dir is empty')
        elif not os.path.isdir(root_dir):
            raise InvalidAttribute('root_dir is not directory')
        
        os.path.walk(root_dir, _upload_dir_cb_, self.bucketName)
        
        return
    
    def download_dir(self, root_dir):
        """
        Downloads content of self.bucketName into a FS tree structure starting at root_dir
        
        @type root_dir: string
        @param root_dir: folder path to download into
        """
        
        if root_dir is '':
            raise InvalidAttribute('root_dir is empty')
        
        if os.path.exists(root_dir) is True and os.path.isdir(root_dir) is True:
            self._reinit_()
            self.get_keys_in_bucket_as_fstree()
            root_dir = os.path.normcase(root_dir)
            root_dir = os.path.normpath(root_dir)
            self._walk_dir_(self.root_dir, _download_dir_cb_, [self.bucketName, root_dir])
            
    def set_policy(self, policy):
        """
        Set policy of self.bucketName
        
        @type policy: string
        @param policy: specified bucket policy
        """
        
        if policy is '' or policy is None:
            raise InvalidAttribute('policy is empty')
        
        metadata = headers = {}
        headers['Content-Length'] = len(policy)
        
        try:
            query_args = {"policy" : None}
            resp = _Response_( self.CONN.make_request( 'PUT', bucket=self.bucketName, key='', 
                                                       query_args=query_args, headers=headers,
                                                       data=policy, metadata=metadata) )
            if (resp._get_status_( ) < 400 ):
                CS.commLog.debug('Set bucket %s policy' % self.bucketName )
                if self.cb:
                    self.cb ('PUT', self.bucketName, '', len(policy))
    
            if ( resp._get_status_( ) >= 400):
                raise CSError(resp._get_status_( ), resp._get_reason_(), 'PUT', self.bucketName, '' )
            
        except Exception, f:
            errLog.error ('ERROR %s' % f )
            raise f
        
        return (resp, hash)
    
    def get_policy(self):
        """
        Get policy of self.bucketName
        
        @rtype: string
        @return: get bucket policy json docs
        """
        
        try:
            query_args = {"policy" : None}
            resp = _GETResponse_(self.CONN.make_request ( 'GET', bucket=self.bucketName, key='', query_args=query_args ))
            status = resp._get_status_()
            reason = resp._get_reason_()
            
            if status == 200:
                CS.commLog.debug('Got bucket %s policy' % self.bucketName)
                
            if ( status >= 400):
                raise CSError(status, reason, 'GET', self.bucketName, '' )
                  
            return resp.body
        
        except Exception, f:
            errLog.error ('ERROR %s' % f )
            raise f
        
    def delete_policy(self):
        """
        Delete policy of self.bucketName
        """
        
        try:
            query_args = {"policy" : None}
            resp = _GETResponse_(self.CONN.make_request ( 'DELETE', bucket=self.bucketName, key='', query_args=query_args ))
            status = resp._get_status_()
            reason = resp._get_reason_()
            
            if status == 204:
                CS.commLog.debug('Delete bucket %s policy' % self.bucketName)
                
            if ( status >= 400):
                raise CSError(status, reason, 'DELETE', self.bucketName, '' )
                  
            return resp.body
        
        except Exception, f:
            errLog.error ('ERROR %s' % f )
            raise f

class SNDA_Object:
    """
    This class encapsulates operations on a specific SNDA CS bucket contents
    """
    def __init__(self, ConnectionObject, bucketName, objectName, cb = _default_cb_):
        """
        SNDA_Object constructor
        
        @type ConnectionObject: SNDAAuthConnection
        @param ConnectionObject: authentication connection with snda grandcloud storage
        @type bucketName: string
        @param bucketName: bucket name
        @type objectName: string
        @param objectName: object name (key name)
        @type cb: function
        @param cb: callback function
        """
        self.CONN = ConnectionObject
        self.bucketName = bucketName
        self.objectName = objectName
        self.cb = cb
    
    class _InitiateMultiuploadResponse_(_Response_):
        
        def __init__(self, http_repsonse):
            _Response_.__init__(self, http_repsonse)
            if self._get_status_() < httplib.MULTIPLE_CHOICES:
                handler = self._InitiateMultiuploadHandler_()
                xml.sax.parseString(self.body, handler)
                self.entries = handler.entries
                if len(self.entries) > 0:
                    self.init_result = self.entries[0]
            else:
                self.entries = []
                self.init_result = None

        class _InitiateMultiuploadHandler_(xml.sax.ContentHandler):
            
            def __init__(self):
                self.entries = []
                self.curr_entry = None
                self.curr_text = ''
                
            def startElement(self, name, attrs):
                if name == 'InitiateMultipartUploadResult':
                    self.curr_entry = self.InitiateMultipartUploadResult()
                    
            def endElement(self, name):
                if name == 'Bucket':
                    self.curr_entry.bucket = self.curr_text
                elif name == 'Key':
                    self.curr_entry.key = self.curr_text
                elif name == 'UploadId':
                    self.curr_entry.upload_id = self.curr_text
                elif name == 'InitiateMultipartUploadResult':
                    self.entries.append(self.curr_entry)
            
            def characters(self, content):
                self.curr_text = content
            
            class InitiateMultipartUploadResult:
                def __init__(self, bucket='', key='', upload_id=''):
                    self.bucket = bucket
                    self.key = key
                    self.upload_id = upload_id
                    
    
    class _ListPartsResponse_(_Response_):
        
        def __init__(self, http_repsonse):
            _Response_.__init__(self, http_repsonse)
            if self._get_status_() < httplib.MULTIPLE_CHOICES:
                handler = self._ListPartsResultHandler_()
                xml.sax.parseString(self.body, handler)
                self.result = handler.result
            else:
                self.result = None
                
        class _ListPartsResultHandler_(xml.sax.ContentHandler):
            
            def __init__(self):
                self.result = None
                self.curr_entry = None
                self.curr_text = ''
                
            def startElement(self, name, attrs):
                if name == 'ListPartsResult':
                    self.result = self.ListPartsResult()
                if name == 'Initiator':
                    self.curr_entry = self.Initiator()
                if name == 'Owner':
                    self.curr_entry = self.Owner()
                if name == 'Part':
                    self.curr_entry = self.Part()
                    
            def endElement(self, name):
                if name == 'Bucket':
                    self.result.bucket = self.curr_text
                if name == 'Key':
                    self.result.key = self.curr_text
                if name == 'UploadId':
                    self.result.upload_id = self.curr_text
                if name == 'Initiator':
                    self.result.initiator = self.curr_entry
                if name == 'Owner':
                    self.result.owner = self.curr_entry
                if name == 'ID':
                    self.curr_entry.id = self.curr_text
                if name == 'DisplayName':
                    self.curr_entry.display_name = self.curr_text
                if name == 'StorageClass':
                    self.result.storage_class = self.curr_text
                if name == 'PartNumberMarker':
                    self.result.part_number_marker = self.curr_text
                if name == 'NextPartNumberMarker':
                    self.result.next_part_number_marker = self.curr_text
                if name == 'MaxParts':
                    self.result.max_parts = self.curr_text
                if name == 'IsTruncated':
                    self.result.is_truncated = self.curr_text
                if name == 'Part':
                    self.result.parts.append(self.curr_entry)
                if name == 'PartNumber':
                    self.curr_entry.part_number = self.curr_text
                if name == 'LastModified':
                    self.curr_entry.last_modified = self.curr_text
                if name == 'ETag':
                    self.curr_entry.etag = self.curr_text
                if name == 'Size':
                    self.curr_entry.size = self.curr_text
                    
            def characters(self, content):
                self.curr_text = content
                    
            class ListPartsResult:
                def __init__(self, bucket='', key='', upload_id=''):
                    self.bucket = bucket
                    self.key = key
                    self.upload_id = upload_id
                    self.initiator = None
                    self.owner = None
                    self.storage_class = None
                    self.part_number_marker = None
                    self.next_part_number_marker = None
                    self.max_parts = None
                    self.is_truncated = None
                    self.parts = []
                    
            class Initiator:
                def __init__(self, id='', dispaly_name=''):
                    self.id = id
                    self.display_name = dispaly_name
                    
            class Owner:
                def __init__(self, id='', display_name=''):
                    self.id = id
                    self.display_name = display_name
                    
            class Part:
                def __init__(self, part_number='', last_modified='', etag='', size=0):
                    self.part_number = part_number
                    self.last_modified = last_modified
                    self.etag = etag
                    self.size = size
                    
    
    class _CompleteMultipartUploadResponse_(_Response_):
        
        def __init__(self, http_repsonse):
            _Response_.__init__(self, http_repsonse)
            if self._get_status_() < httplib.MULTIPLE_CHOICES:
                handler = self._CompleteMultipartUploadResultHandler_()
                xml.sax.parseString(self.body, handler)
                self.result = handler.result
            else:
                self.result = None
                
        class _CompleteMultipartUploadResultHandler_(xml.sax.ContentHandler):
            
            def __init__(self):
                self.result = None
                self.curr_text = ''
            
            def startElement(self, name, attrs):
                if name == 'CompleteMultipartUploadResult':
                    self.result = self.CompleteMultipartUploadResult()
            
            def endElement(self, name):
                if name == 'Bucket':
                    self.result.bucket = self.curr_text
                if name == 'Key':
                    self.result.key = self.curr_text
                if name == 'Location':
                    self.result.location = self.curr_text
                if name == 'ETag':
                    self.result.etag = self.curr_text
            
            def characters(self, content):
                self.curr_text = content
                
            class CompleteMultipartUploadResult:
                def __init__(self, bucket='', key='', location='', etag=''):
                    self.bucket = bucket
                    self.key = key
                    self.location = location
                    self.etag = etag
                    
    def _stream_data_to_stream_(self):
        """
        Private method that retrieves (if it exists!) the ECS object identified by bucketName+keyName
        """
        try:
            response = self.CONN.make_request ( 'GET', bucket=self.bucketName, key=self.objectName, \
                                                               headers={} )
            status = response.status
            if status < 400:
                appLog.debug('Got %s bytes' % response.getheader('Content-Length'))
                
            if self.cb:
                self.cb('GET', self.bucketName, self.objectName, response.getheader('Content-Length'))
            
            return response
        except Exception, f:
            errLog.error('ERROR %s' % f)
            raise f
        
    def _stream_data_to_file_(self, fileName, file_size):
        """
        Private method that retrieves (if it exists!) the ECS object identified by bucketName+keyName 
        to the file identified by fileName
        
        @type fileName: string
        @param fileName: file name
        @type file_size: number
        @param file_size: file size
        """
        if not fileName:
            raise InvalidAttribute('fileName')
        
        numTries = fp = start = resp = local_hash = 0
        if int(file_size) < Util.CHUNK_SIZE:
            end = int(file_size) - 1
        else:
            end = Util.CHUNK_SIZE - 1
        
        if Config.CSProperties['CheckHash'] == 'True':
            fileHash = md5()
            
        try:
            while True:
                byteRange = 'bytes=%d-%d' % (start, end)
                resp = _GETResponse_(self.CONN.make_request ( 'GET', bucket=self.bucketName, key=self.objectName, \
                                                               headers={"Range":byteRange} ))
                status = resp._get_status_()
                reason = resp._get_reason_()
                
                if status < 400:
                    appLog.debug('Got %s bytes' % resp._get_content_length_())
                
                if self.cb:
                    self.cb('GET', self.bucketName, self.objectName, resp._get_content_length_())
                    
                if not fp:
                    fp = open(fileName, 'wb')
                    
                fp.write(resp._get_data_())
                
                if Config.CSProperties['CheckHash'] == 'True':
                    fileHash.update(resp._get_data_())
                    
                start = fp.tell()
                end = start + Util.CHUNK_SIZE - 1
                if end > long(file_size) - 1:
                    end = long(file_size) - 1
                
                if start >= long(file_size):
                    break
                
                if int(resp._get_content_length_()) != Util.CHUNK_SIZE:
                    break
                
        except Exception, f:
            errLog.error('ERROR %s' % f)
            raise f
        finally:
            if fp:
                fp.flush()
                fp.close()
                
            if Config.CSProperties['CheckHash'] == 'True':
                local_hash = fileHash.hexdigest()
                
            return resp, local_hash
        
    def _stream_data_from_stream_(self, size, stream, headers, metadata):
        resp = 0
        
        try:
            if not headers.has_key('Content-Type'):
                extension = self.objectName.split('.')[-1]
                headers['Content-Type'] = Types.get_type(extension.lower())
                
            headers['Content-Length'] = size
            
            if self.CONN.is_secure:
                connection = httplib.HTTPSConnection('%s:%d' % (self.CONN.server, self.CONN.port))
            else:
                connection = httplib.HTTPConnection('%s:%d' % (self.CONN.server, self.CONN.port))
                
            self.CONN.prepare_message('PUT', self.bucketName, self.objectName, {}, headers, metadata)
            
            CS.commLog.debug('RQST MSG:%s --details follow--\n\t%s - %s:%d\n\tHeaders:%s' % \
                             ('PUT', self.CONN.path, connection.host, connection.port, self.CONN.final_headers))
            
            connection.putrequest('PUT', self.CONN.path)
            
            for key in self.CONN.final_headers.keys():
                connection.putheader(key, self.CONN.final_headers[key])
                
            connection.endheaders()
            connection.send(stream.read())
                
            resp = _Response_( connection.getresponse( ) )

            CS.commLog.debug('RESP MSG: Status=%d(%s).  ---msg follows---\n%s' % \
                             (resp._get_status_(), resp._get_reason_(), resp._get_msg_()  ) )
            
            if ( resp._get_status_( ) == 301 ):
                raise CSError(resp._get_status_( ), resp._get_reason_(), 'PUT', self.bucketName, self.objectName )

            if ( resp._get_status_( ) < 400 ):
                CS.commLog.debug('Sent %d bytes' % size )
                if self.cb:
                    self.cb ('PUT', self.bucketName, self.objectName, size)

            if ( resp._get_status_( ) >= 400):
                raise CSError(resp._get_status_( ), resp._get_reason_(), 'PUT', self.bucketName, self.objectName )

        except Exception, f:
            errLog.error ('ERROR %s' % f )
            raise f
        finally:
            connection.close()

        return resp
    
    def _stream_data_from_file_(self, fileName, headers):
        """
        Private method that uploads (if it exists!) to the ECS object identified by bucketName+keyName 
        to the contents in the file identified by fileName
        
        @type fileName: string
        @param fileName: file name
        """
        fp = resp = hash = 0
        metadata = {}
        if headers is None:
            headers = {}
        try:
            if os.path.exists(fileName) == False:
                raise CSNoSuchFile(fileName)
            
            if Config.CSProperties['CheckHash'] == 'True':
                fileHash = md5()
            
            #open file    
            fp = open(fileName, 'rb')
            
            #get file size
            size = _get_file_size_(fp)
            
            if not headers.has_key('Content-Type'):
                extension = self.objectName.split('.')[-1]
                headers['Content-Type'] = Types.get_type(extension.lower())
                
            headers['Content-Length'] = size
            
            file_info = os.stat(fileName)
            metadata[_MD_LAST_MODIFIED_TIME_] = \
                    time.strftime(Util.CS_TIME_FORMAT, time.gmtime(file_info.st_mtime))
            metadata[_MD_CREATE_TIME_] = \
                    time.strftime(Util.CS_TIME_FORMAT, time.gmtime(file_info.st_ctime))
            metadata[_MD_SIZE_] = str(file_info.st_size)
                    
            if self.CONN.is_secure:
                connection = httplib.HTTPSConnection('%s:%d' % (self.CONN.server, self.CONN.port))
            else:
                connection = httplib.HTTPConnection('%s:%d' % (self.CONN.server, self.CONN.port))
                
            self.CONN.prepare_message('PUT', self.bucketName, self.objectName, {}, headers, metadata)
            
            CS.commLog.debug('RQST MSG:%s --details follow--\n\t%s - %s:%d\n\tHeaders:%s' % \
                             ('PUT', self.CONN.path, connection.host, connection.port, self.CONN.final_headers))
            
            connection.putrequest('PUT', self.CONN.path)
            
            for key in self.CONN.final_headers.keys():
                connection.putheader(key, self.CONN.final_headers[key])
                
            connection.endheaders()
            
            while True:
                if (size == 0):
                    CS.commLog.debug ('Zero length file(%s), uploaded to %s(%s)' % \
                                      (fileName, self.bucketName, self.objectName) )                    
                bytes = fp.read(Util.CHUNK_SIZE)
                
                if not bytes:
                    break
 
                if (Config.CSProperties['CheckHash'] == 'True'):
                    fileHash.update ( bytes )
                    
                length = len(bytes)
                connection.send(bytes)
                
            if size == 0:
                length = size
                
            if (Config.CSProperties['CheckHash'] == 'True'):
                hash = base64.encodestring(fileHash.digest());
                
            resp = _Response_( connection.getresponse( ) )

            CS.commLog.debug('RESP MSG: Status=%d(%s).  ---msg follows---\n%s' % \
                             (resp._get_status_(), resp._get_reason_(), resp._get_msg_()  ) )
            
            if ( resp._get_status_( ) == 301 ):
                redirect_server = urlparse(resp.http_response.getheader('Location')).hostname
                if self.CONN.is_secure:
                    connection = httplib.HTTPSConnection('%s:%d' % (redirect_server, self.CONN.port))
                else:
                    connection = httplib.HTTPConnection('%s:%d' % (redirect_server, self.CONN.port))
                connection.putrequest('PUT', self.CONN.path)
            
                for key in self.CONN.final_headers.keys():
                    connection.putheader(key, self.CONN.final_headers[key])
                    
                connection.endheaders()
                
                #open file    
                fp = open(fileName, 'rb')
                while True:
                    if (size == 0):
                        CS.commLog.debug ('Zero length file(%s), uploaded to %s(%s)' % \
                                          (fileName, self.bucketName, self.objectName) )                    
                    bytes = fp.read(Util.CHUNK_SIZE)
                    
                    if not bytes:
                        break
     
                    if (Config.CSProperties['CheckHash'] == 'True'):
                        fileHash.update ( bytes )
                        
                    length = len(bytes)
                    connection.send(bytes)
                    
                if size == 0:
                    length = size
                    
                if (Config.CSProperties['CheckHash'] == 'True'):
                    hash = base64.encodestring(fileHash.digest());
                    
                resp = _Response_( connection.getresponse( ) )

            if ( resp._get_status_( ) < 400 ):
                CS.commLog.debug('Sent %d bytes' % length )
                if self.cb:
                    self.cb ('PUT', self.bucketName, self.objectName, length)

            if ( resp._get_status_( ) >= 400):
                raise CSError(resp._get_status_( ), resp._get_reason_(), 'PUT', self.bucketName, self.objectName )

        except Exception, f:
            errLog.error ('ERROR %s' % f )
            raise f
        finally:
            if fp:
                fp.close( )

        return (resp, hash)
    
    def put_object(self, size, stream, headers, metadata):
        """
        Streams a local file idenfied by fileName to the ECS object identified 
        by bucketName+keyName.
        
        @type size: int
        @param size: stream size
        @type stream: stream
        @param stream: input stream
        @type headers: dictionary
        @param headers: http headers
        @type metadata: dictionary
        @param metadata: meta data
        """
        
        numTries = 0
        
        while numTries < _NUMBER_OF_RETRIES_:
            try:
                resp = self._stream_data_from_stream_(size, stream, headers=headers, metadata=metadata)
                if resp._get_status_( ) == 204:
                    return resp
                else:
                    break
            except CSNoSuchFile, e:
                errLog.error(str(e))
                raise e
            except Exception, f:
                errLog.error ('ERROR %s' % f )
                numTries += 1
            
        return resp

    def put_object_from_file(self, fileName, headers=None):
        """
        Streams a local file idenfied by fileName to the ECS object identified 
        by bucketName+keyName.
        
        @type fileName: string
        @param fileName: file name
        """
        
        numTries = fp = start = end = 0
        
        while numTries < _NUMBER_OF_RETRIES_:
            try:
                resp, h = self._stream_data_from_file_(fileName, headers)
                if resp._get_status_( ) == 204:
                    break
                if Config.CSProperties['CheckHash'] == 'True':
                    if resp._get_etag_() != h:
                        errLog.error('Invalid hash. Local:%s, Remote:%s on uploading file %s. Retry Count=%d' % \
                                       (h, resp._get_etag_(), fileName, numTries ))
                        numTries += 1
                else:
                    break
            except CSNoSuchFile, e:
                errLog.error(str(e))
                raise e
            except Exception, f:
                errLog.error ('ERROR %s' % f )
                numTries += 1
            
        return
    
    def get_object_info(self):
        """
        Get's all available information about the CS object identified by self.bucketName +
        self.keyName and returns a _GETResponse_ object or raises CSNotFound exception
        """
        try:
            resp = _GETResponse_(self.CONN.make_request(method = 'HEAD', bucket=self.bucketName, key=self.objectName))
            
        except CSError, e:
            errLog.debug(str(e))
            raise CSNotFound(self.bucketName, self.objectName)
        except Exception, f:
            errLog.debug('ERROR %s' % f)
            raise f
        
        return resp
    
    def get_object(self):
        """
        Retrieves (if it exists!) the ECS object identified by self.bucketName +
        self.keyName.
        """
        numTries = 0
        while numTries < _NUMBER_OF_RETRIES_:
            try:
                object_info = self.get_object_info()
                if object_info._get_content_length_() > 0:
                    response = self._stream_data_to_stream_()
                    
                    if (response.status is httplib.OK) \
                        or (response.status is httplib.PARTIAL_CONTENT):
                        break
                    
            except CSNotFound, e:
                errLog.debug(str(e))
                raise e
            except Exception, f:
                errLog.error('ERROR %s' % f)
                numTries += 1
                
        return response
    
    def get_object_to_file(self, fileName):
        """
        Retrieves (if it exists!) the ECS object identified by self.bucketName +
        self.keyName to the file identified by fileName.
        
        @type fileName: string
        @param fileName: file name
        """
        numTries = 0
        while numTries < _NUMBER_OF_RETRIES_:
            try:
                object_info = self.get_object_info()
                if object_info._get_content_length_() > 0:
                    resp, h = self._stream_data_to_file_(fileName, object_info._get_content_length_())
                    
                    if (resp._get_status_() is httplib.OK) \
                        or (resp._get_status_() is httplib.PARTIAL_CONTENT):
                        break
                    
                    if Config.CSProperties['CheckHash'] == 'True':
                        if resp._get_etag_() != h:
                            appLog.warning('Invalid hash. Local:%s, Remote:%s on uploading file %s. Retry Count=%d' % \
                                            (h, resp._get_etag_(), fileName, numTries ))
                            numTries += 1
                        else:
                            break
                else:
                    # Handle the case of a zero-length file
                    fp = open(fileName, 'wb')
                    fp.flush()
                    fp.close()
                    break
            except CSNotFound, e:
                errLog.debug(str(e))
                raise e
            except Exception, f:
                errLog.error('ERROR %s' % f)
                numTries += 1
                
        return
    
    def get_object_to_screen_printing(self):
        """
        not supported
        """
        pass
    
    def delete_object(self):
        """
        Delete the ECS object (if it exists).
        """
        try:
            resp = _GETResponse_(self.CONN.make_request(method = 'DELETE', bucket=self.bucketName, key=self.objectName))
            
        except Exception, f:
            errLog.debug('ERROR %s' % f)
            raise f
        
        return resp
    
    def sync_upload_from_file(self, fileName):
        """
        Uploads file specified by fileName if it is different than what exists on ECS
        Returns True if the file was actually uploaded.
        
        @type fileName: string
        @param fileName: file name
        """
        fUpload = True
        try:
            object_info  = self.get_object_info( )
            file_info = os.stat( fileName)

            if ( file_info.st_size == int ( object_info._get_metadata_value_( CS.METADATA_PREFIX + 'content-length' ) ) ):
                # The file sizes are the same, next check the last modified times
                last_modified = time.strftime( Util.CS_TIME_FORMAT, time.gmtime( file_info.st_mtime ) )
                if (time.strftime( Util.CS_TIME_FORMAT, time.gmtime( file_info.st_mtime ) ) == \
                    object_info._get_metadata_value_ ( CS.METADATA_PREFIX + _MD_LAST_MODIFIED_TIME_ ) ):
                    # The last modified times are also identical, let's check the hash as a last resort
                    hash = Util.get_hash_from_filename( fileName )        
                    if (hash == object_info._get_etag_( ) ):
                        fUpload = False

            if (fUpload):
                self.put_object_from_file ( fileName )

        except Exception, f:
            errLog.debug ('ERROR %s' % f)
            raise f
        else:
            fSuccess = True
        finally:
            pass

        return ( fUpload )
        
    def sync_download_to_file(self, fileName):
        """
        DOWNLOADS to file specified by fileName if it is different than what exists on ECS
        Returns True if the file was actually downloaded.
        
        @type fileName: string
        @param fileName: file name
        """
        fDownload = True
        try:
            object_info  = self.get_object_info( )
            if (os.path.exists ( fileName) == True):
                file_info = os.stat( fileName)

                if ( file_info.st_size == int ( object_info._get_metadata_value_( CS.METADATA_PREFIX + _MD_SIZE_ ) ) ):
                    # The file sizes are the same, next check the last modified times
                    last_modified = time.strftime( Util.CS_TIME_FORMAT, time.gmtime( file_info.st_mtime ) )
                    if (time.strftime( Util.CS_TIME_FORMAT, time.gmtime( file_info.st_mtime ) ) == \
                        object_info._get_metadata_value_ ( CS.METADATA_PREFIX + _MD_LAST_MODIFIED_TIME_) ):
                        # The last modified times are also identical, let's check the hash as a last resort
                        hash = Util.get_hash_from_filename( fileName )        
                        if (hash == object_info._get_etag_( ) ):
                            fDownload = False

            else:
                fDownload = True
                
            if (fDownload):
                self.get_object_to_file ( fileName )

        except Exception, f:            
            errLog.error ('ERROR %s' % f)
            raise f
        else:
            fSuccess = True
        finally:
            pass

        return ( fDownload )
    
    def _initiate_multipart_upload_(self):
        """
        Initiate to upload a file with multiparts
        """
        query_args = {"uploads" : None}
        try:
            resp = self._InitiateMultiuploadResponse_(self.CONN.make_request(method = 'POST', bucket=self.bucketName, key=self.objectName, query_args=query_args))
            
            if resp._get_status_( ) == 200:
                CS.commLog.debug('Initiate key /%s/%s as multiupload.' % (self.bucketName, self.objectName))
                self.init_result = resp.init_result
                
            if ( resp._get_status_( ) >= 400):
                raise CSError(resp._get_status_( ), resp._get_reason_( ), 'POST', self.bucketName, self.objectName )
            
            
        except Exception, f:
            errLog.debug ('ERROR %s' % f)
            raise f
        
    def _upload_part_from_file_(self, upload_id, part_number, fileName):
        """
        Uploads a part from file in a multipart upload session.
        
        @type upload_id: string
        @param upload_id: upload id identifying the multipart upload
        @type part_number: string
        @param part_number: part number that identifies the part
        @type fileName: string
        @param fileName: file name
        """
        abort_id = None
        if upload_id:
            abort_id = upload_id
        elif hasattr(self, "init_result") and not self.init_result and not self.init_result.upload_id:
            abort_id = self.init_result.upload_id
        query_args = {"uploadId" : abort_id,
                      "partNumber" : part_number}
        
        fp = resp = hash = 0
        metadata = headers = {}
        try:
            if os.path.exists(fileName) == False:
                raise CSNoSuchFile(fileName)
            
            if Config.CSProperties['CheckHash'] == 'True':
                fileHash = md5()
            
            #open file    
            fp = open(fileName, 'rb')
            
            #get file size
            size = _get_file_size_(fp)
            
#            headers['Content-Type'] = 'application/octet-stream'
            headers['Content-Length'] = size
#            headers['Expect'] = '100-continue'
#            headers['Connection'] = 'keep-alive'
            
#            file_info = os.stat(fileName)
#            metadata[_MD_LAST_MODIFIED_TIME_] = \
#                    time.strftime(Util.CS_TIME_FORMAT, time.gmtime(file_info.st_mtime))
#            metadata[_MD_CREATE_TIME_] = \
#                    time.strftime(Util.CS_TIME_FORMAT, time.gmtime(file_info.st_ctime))
#            metadata[_MD_SIZE_] = str(file_info.st_size)
                    
            if self.CONN.is_secure:
                connection = httplib.HTTPSConnection('%s:%d' % (self.CONN.server, self.CONN.port))
            else:
                connection = httplib.HTTPConnection('%s:%d' % (self.CONN.server, self.CONN.port))
                
            self.CONN.prepare_message('PUT', self.bucketName, self.objectName, query_args, headers, metadata)
            
            CS.commLog.debug('RQST MSG:%s --details follow--\n\t%s - %s:%d\n\tHeaders:%s' % \
                             ('PUT', self.CONN.path, connection.host, connection.port, self.CONN.final_headers))
            
            connection.putrequest('PUT', self.CONN.path)
            
            for key in self.CONN.final_headers.keys():
                connection.putheader(key, self.CONN.final_headers[key])
                
            connection.endheaders()
            
            while True:
                if (size == 0):
                    CS.commLog.debug ('Zero length file(%s), uploaded to %s(%s)' % \
                                      (fileName, self.bucketName, self.objectName) )                    
                bytes = fp.read(Util.CHUNK_SIZE)
                
                if not bytes:
                    break
 
                if (Config.CSProperties['CheckHash'] == 'True'):
                    fileHash.update ( bytes )
                    
                length = len(bytes)
                connection.send(bytes)
                
            if size == 0:
                length = size
                
            if (Config.CSProperties['CheckHash'] == 'True'):
                hash = base64.encodestring(fileHash.digest());
                
            resp = _Response_( connection.getresponse( ) )

            CS.commLog.debug('RESP MSG: Status=%d(%s).  ---msg follows---\n%s' % \
                             (resp._get_status_(), resp._get_reason_(), resp._get_msg_()  ) )
            
            if (resp._get_status_( ) < 400 ):
                CS.commLog.debug('Sent %d bytes' % length )
                if self.cb:
                    self.cb ('PUT', self.bucketName, self.objectName, length)
                return Part(part_number, resp._get_etag_with_quote_())

            if ( resp._get_status_( ) >= 400):
                raise CSError(resp._get_status_( ), resp._get_reason_(), 'PUT', self.bucketName, self.objectName )

        except Exception, f:
            errLog.error ('ERROR %s' % f )
            raise f
        finally:
            if fp:
                fp.close( )

    def _upload_part_from_data_(self, upload_id, part_number, data):
        """
        Uploads a part in a multipart upload session.
        
        @type upload_id: string
        @param upload_id: upload id identifying the multipart upload
        @type part_number: string
        @param part_number: part number that identifies the part
        @type data: bytes
        @param data: contents to upload
        """
        abort_id = None
        if upload_id:
            abort_id = upload_id
        elif hasattr(self, "init_result") and not self.init_result and not self.init_result.upload_id:
            abort_id = self.init_result.upload_id
        query_args = {"uploadId" : abort_id,
                      "partNumber" : part_number}
        headers = {}
        try:
            resp = _Response_(self.CONN.make_request(method = 'PUT', bucket=self.bucketName, key=self.objectName, headers=headers, query_args=query_args, data=data))
            if resp._get_status_( ) == 204:
                CS.commLog.debug('Sent %d bytes' % len(data))
                return Part(part_number, resp._get_etag_with_quote_())
        except Exception, f:
            errLog.error ('ERROR %s' % f )
            raise f
            
        
    def _abort_multipart_upload_(self, upload_id):
        """
        Aborts a multiupload.
        
        @type upload_id: string
        @param upload_id: upload id identifying the multipart upload
        """
        abort_id = None
        if upload_id:
            abort_id = upload_id
        elif hasattr(self, "init_result") and not self.init_result and not self.init_result.upload_id:
            abort_id = self.init_result.upload_id
        query_args = {"uploadId" : abort_id}
        try:
            resp = _Response_(self.CONN.make_request(method = 'DELETE', bucket=self.bucketName, key=self.objectName, query_args=query_args))
            
            if resp._get_status_( ) == 204:
                CS.commLog.debug('Abort multiupload of uploadId %s.' % abort_id)
                
        except CSError, e:
            if e.status == 404:
                raise CSNoSuchUpload(abort_id)
            raise e
            
        except Exception, f:
            errLog.debug ('ERROR %s' % f)
            raise f
        
    def _complete_multipart_upload_(self, upload_id, complete_parts):
        """
        Completes a multipart upload by assembling previously uploaded parts
        
        @type upload_id: string
        @param upload_id: upload id identifying the multipart upload
        @type complete_parts: string
        @param complete_parts: complete parts xml contents
        """
        abort_id = None
        if upload_id:
            abort_id = upload_id
        elif hasattr(self, "init_result") and not self.init_result and not self.init_result.upload_id:
            abort_id = self.init_result.upload_id
        query_args = {"uploadId" : abort_id}
        try:
            resp = self._CompleteMultipartUploadResponse_(self.CONN.make_request(method = 'POST', bucket=self.bucketName, key=self.objectName, query_args=query_args, data=complete_parts))
            self.complete_parts_result = resp.result
        except CSError, e:
            if e.status == 404:
                raise CSNoSuchUpload(abort_id)
            raise e
        
        except Exception, f:
            errLog.debug ('ERROR %s' % f)
            raise f
        
    def _list_parts_(self, upload_id, max_parts=None, part_number_marker=None):
        """
        Lists parts that have been uploaded for a specific multipart upload.
        
        @type upload_id: string
        @param upload_id: upload id identifying the multipart upload
        @type max_parts: number
        @param max_parts: maximum number of parts to return in the response body
        @type part_number_marker: number
        @param part_number_marker: specifies the part after which listing should begin
        """
        abort_id = None
        if upload_id:
            abort_id = upload_id
        elif hasattr(self, "init_result") and not self.init_result and not self.init_result.upload_id:
            abort_id = self.init_result.upload_id
        query_args = {"uploadId" : abort_id}
        if max_parts:
            query_args["max-parts"] = max_parts
        if part_number_marker:
            query_args["part-number-marker"] = part_number_marker
        try:
            resp = self._ListPartsResponse_(self.CONN.make_request(method = 'GET', bucket=self.bucketName, key=self.objectName, query_args=query_args))
            
            if resp._get_status_( ) == 200:
                self.list_parts_result = resp.result
                
        except CSError, e:
            if e.status == 404:
                raise CSNoSuchUpload(abort_id)
            raise e
        
        except Exception, f:
            errLog.debug ('ERROR %s' % f)
            raise f