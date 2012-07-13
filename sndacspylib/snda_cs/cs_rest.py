'''
Created on 2011-7-27
'''

from sndacspylib.snda_cs_exception import *
from sndacspylib.snda_cs_genutilities import CS_TIME_FORMAT

from urlparse import urlparse
import base64
import hashlib
import hmac
import httplib
import logging
import socket
import time
import urllib2


DEFAULT_HOST = 'storage.grandcloud.cn'
SIGNEDURL_HOST = 'storage.sdcloud.cn'
PORTS_BY_SECURITY = { True: 443, False: 80 }
METADATA_PREFIX = 'x-snda-meta-'
SNDA_HEADER_PREFIX = 'x-snda-'

commLog = logging.getLogger('comm')
errLog = logging.getLogger('err')

SUB_RESOURCE = {'lifecycle':True,
                'location':True,
                'logging':True,
                'partNumber':True,
                'policy':True,
                'uploadId':True,
                'uploads':True,
                'versionId':True,
                'versioning':True,
                'versions':True,
                'website':True,}

def canonical_string(method, bucket='', key='', query_args={}, headers={}, expires=None):
    """
    Generate string which should be signed and setted in header while sending request
    
    @type     method: string
    @param    method: http request method
    @type     bucket: string
    @param    bucket: bucket name 
    @type     key:    string
    @param    key:    key name
    @type     query_args: dictionary
    @param    query_args: url query arguments
    @type     headers: dictionary
    @param    headers: http request headers
    @type     expires: boolean
    @param    expires: config expires or not
    
    @rtype: string
    @return: canonical string for grandcloud storage service 
    """
    interesting_headers = {}
    for header_key in headers:
        lk = header_key.lower()
        if lk in ['content-md5', 'content-type', 'date'] or lk.startswith(SNDA_HEADER_PREFIX):
            interesting_headers[lk] = str(headers[header_key]).strip()

    # these keys get empty strings if they don't exist
    if not interesting_headers.has_key('content-type'):
        interesting_headers['content-type'] = ''
    if not interesting_headers.has_key('content-md5'):
        interesting_headers['content-md5'] = ''

    # just in case someone used this.  it's not necessary in this lib.
    if interesting_headers.has_key('x-snda-date'):
        interesting_headers['date'] = ''

    # if you're using expires for query string auth, then it trumps date
    # (and x-snda-date)
    if expires:
        interesting_headers['date'] = str(expires)

    sorted_header_keys = interesting_headers.keys()
    sorted_header_keys.sort()

    buf = "%s\n" % method
    for header_key in sorted_header_keys:
        if header_key.startswith(SNDA_HEADER_PREFIX):
            buf += "%s:%s\n" % (header_key, interesting_headers[header_key])
        else:
            buf += "%s\n" % interesting_headers[header_key]
            
    #append the root path
    buf += '/'

    # append the bucket if it exists
    if bucket != "":
        buf += "%s" % bucket

    # add the key.  even if it doesn't exist, add the slash
    if key != "":
        buf += "/%s" % urllib2.quote(key)
        #buf += "/%s" % key

    # handle special query string arguments

#    if query_args.has_key("acl"):
#        buf += "?acl"
#    elif query_args.has_key("torrent"):
#        buf += "?torrent"
#    elif query_args.has_key("logging"):
#        buf += "?logging"
#    elif query_args.has_key("policy"):
#        buf += "?policy"
#    elif query_args.has_key("uploads"):
#        buf += "?uploads"
    
    if len(query_args) > 0:
        buf += "?"
        query_string = ""
        pairs = []
        for k, v in query_args.items():
            if not SUB_RESOURCE.has_key(k):
                continue
            piece = k
            if v != None:
                piece += "=%s" % urllib2.quote(str(v))
            pairs.append(piece)
    
        buf += '&'.join(pairs)
        if len(pairs) == 0:
            return buf.rstrip('?')

    return buf
    

def encode(access_key_secret, data, urlencode=False):
    """
    Encode data with secret key using HMAC-SHA1,
    Convert the encoding byte to base64 codec.
    
    @type access_key_secret: string
    @param access_key_secret: secret key
    @type data: bytes
    @param data: encrypt data
    @type urlencode: boolean
    @param urlencode: encode url or not
    
    @rtype: string
    @return: base64 encode encryption data which method is HMAC-SHA1
    """
    b64_hmac_sha1 = base64.encodestring(hmac.new(str(access_key_secret), data, hashlib.sha1).digest()).strip()
    if urlencode:
        return urllib2.quote(b64_hmac_sha1)
    else:
        return b64_hmac_sha1

def merge_meta(headers, metadata):
    """
    Merge meta data with "x-snda-meta-" prefix to http request headers
    
    @type headers: dictionary
    @param headers: http request headers
    @type metadata: dictionary
    @param metadata: meta data
    
    @rtype: dictionary
    @return: merged headers
    """
    final_headers = headers.copy()
    for brief_header in metadata:
        final_headers[METADATA_PREFIX + brief_header] = metadata[brief_header]
        
    return final_headers

# builds the query arg string
def query_args_hash_to_string(query_args):
    """
    Combine url query strings
    
    @type query_args: dictionary
    @param query_args: url query arguments
    
    @rtype: string
    @return: url query string
    """
    query_string = ""
    pairs = []
    for k, v in query_args.items():
        piece = k
        if v != None:
            piece += "=%s" % urllib2.quote(str(v))
        pairs.append(piece)

    return '&'.join(pairs)

class CallingFormat:
    REGULAR = 1
    SUBDOMAIN = 2
    
    @staticmethod
    def build_url_base(protocol, server, port, bucket, calling_format):
        """
        Build base url string
        
        @type protocol: string
        @param protocol: HTTP or HTTPS
        @type server: string
        @param server: service uri
        @type port: number
        @param port: service port
        @type calling_format: number
        @param calling_format: CallingFormat.REGULAR(1) or CallingFormat.SUBDOMAIN(2)
        
        @rtype: string
        @return: base url string
        """
        url_base = '%s://' % protocol
        
        if bucket == '':
            url_base += server
        elif calling_format == CallingFormat.SUBDOMAIN:
            url_base += '%s.%s' % (bucket, server)
        else:
            url_base += bucket
        
        url_base += ':%s' % port
        
        if (bucket != '') and (calling_format == CallingFormat.REGULAR):
            url_base += '/%s' % bucket

        return url_base

class SNDAAuthConnection:
    """
    This class encapsulates operations on a specific SNDA CS Authentication contents
    """
    def __init__(self, access_key_id, access_key_secret, is_secure=False,
                 server=DEFAULT_HOST, port=None, calling_format=CallingFormat.REGULAR):
        """
        SNDAAuthConnection constructor
        
        @type access_key_id: string
        @param access_key_id: access key
        @type access_key_secret: string
        @param access_key_secret: secret key
        @type is_secure: boolean
        @param is_secure: use HTTPS or not
        @type server: string
        @param server: service uri
        @type port: number
        @param port: service port
        @type calling_format: number
        @param calling_format: CallingFormat.REGULAR(1) or CallingFormat.SUBDOMAIN(2)
        """
        if not port:
            port = PORTS_BY_SECURITY[is_secure]
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.is_secure = is_secure
        self.server = server
        self.port = port
        self.calling_format = calling_format
        self.path = ''
        self.final_headers = {}
        
        if commLog.level == logging.DEBUG:
            httplib.HTTPConnection.debuglevel = 1
            
    def prepare_message(self, method, bucket='', key='', query_args={}, headers={}, metadata={}):
        """
        Prepare url string and request header with authentication.
        
        @type     method: string
        @param    method: http request method
        @type     bucket: string
        @param    bucket: bucket name 
        @type     key:    string
        @param    key:    key name
        @type     query_args: dictionary
        @param    query_args: url query arguments
        @type     headers: dictionary
        @param    headers: http request headers
        @type     metadata: dictionary
        @param    metadata: meta data
        """
        self.path = '/'
        if (bucket != '') and (self.calling_format == CallingFormat.REGULAR):
            self.path += '%s' % bucket
            
        #if needed to do the quote?
        if key != '':
            #self.path += '/%s' % key
            self.path += '/%s' % urllib2.quote(key)
        #or
        #self.path += '%s' % urllib.quote_plus(key)
        
        #deal with the query_args
        # build the path_argument string
        # add the ? in all cases since 
        # signature and credentials follow path args
        self.path += "?"
        self.path += query_args_hash_to_string(query_args)
        
        #
        #TODO
        self.final_headers = merge_meta(headers, metadata)
        if key == '' and not query_args.has_key('policy'):
            if not headers.has_key('Content-Length'):
                self.final_headers['Content-Length'] = '0'
        
        #add auth header
        self.add_cs_auth_header(self.final_headers, method, bucket, key, query_args)
        
    def add_cs_auth_header(self, headers, method, bucket, key, query_args):
        """
        Add authentication header in http request headers group.
        
        @type     method: string
        @param    method: http request method
        @param    bucket: bucket name 
        @type     key:    string
        @param    key:    key name
        @type     query_args: dictionary
        @param    query_args: url query arguments
        @type     headers: dictionary
        @param    headers: http request headers
        """
        if not headers.has_key('Date'):
            headers['Date'] = time.strftime(CS_TIME_FORMAT, time.gmtime())
        
        c_string = canonical_string(method, bucket, key, query_args, headers)
        authorization_string = encode(self.access_key_secret, c_string)
        headers['Authorization'] = \
            'SNDA %s:%s' % (self.access_key_id, authorization_string)
            
        commLog.debug('canonical string: %r\nAuthN: %s' % (c_string, headers['Authorization'] ) )
        
    def make_request(self, method, bucket='', key='', query_args={}, headers={}, data='', metadata={}):
        """
        Send http request and get http response
        
        @type     method: string
        @param    method: http request method
        @type     bucket: string
        @param    bucket: bucket name 
        @type     key:    string
        @param    key:    key name
        @type     query_args: dictionary
        @param    query_args: url query arguments
        @type     headers: dictionary
        @param    headers: http request headers
        @type     metadata: dictionary
        @param    metadata: meta data
        @type     data: bytes
        @param    data: request body, contents
        """
        try:
            self.prepare_message(method, bucket, key, query_args, headers, metadata)
            
            if self.is_secure:
                connection = httplib.HTTPSConnection("%s:%d" % (self.server, self.port))
            else:
                connection = httplib.HTTPConnection("%s:%d" % (self.server, self.port))
                
            commLog.debug('RQST MSG:%s --details follow--\n\t%s - %s:%d\n\tHeaders:%s' % \
                          (method, self.path, connection.host, connection.port, self.final_headers))
            
            connection.request(method, self.path, data, self.final_headers)
            response = connection.getresponse()
            
            commLog.debug('RESP MSG: Status=%d(%s).  ---msg follows---\n%s' % \
                       (response.status, response.reason, response.msg))
            
            if response.status == 301:
                redirect_server = urlparse(response.getheader('Location')).hostname
                if self.is_secure:
                    connection = httplib.HTTPSConnection("%s:%d" % (redirect_server, self.port))
                else:
                    connection = httplib.HTTPConnection("%s:%d" % (redirect_server, self.port))
                connection.request(method, self.path, data, self.final_headers)
                response = connection.getresponse()
                
                commLog.debug('RESP MSG: Status=%d(%s).  ---msg follows---\n%s' % \
                           (response.status, response.reason, response.msg))
                
        except socket.error, (value, message):
            errLog.error('Caught %d:%s. Aborting' % (value, message))
            raise
        except Exception, f:
            errLog.error('ERROR %s' % f)
            raise f
        
        return response
    
    def create_signed_url(self, method, bucket='', key='', query_args={}, headers={}, metadata={}, expire=0):
        self.path = '/'
        if (bucket != '') and (self.calling_format == CallingFormat.REGULAR):
            self.path += '%s' % bucket
            
        #if needed to do the quote?
        if key != '':
            self.path += '/%s' % urllib2.quote(key)
            
        self.path += "?"
        self.path += query_args_hash_to_string(query_args)
        
        self.final_headers = merge_meta(headers, metadata)
        if key == '' and not query_args.has_key('policy'):
            if not headers.has_key('Content-Length'):
                self.final_headers['Content-Length'] = '0'
        
        if not headers.has_key('Date'):
            headers['Date'] = time.strftime(CS_TIME_FORMAT, time.gmtime())
        
        #calculate auth
        c_string = canonical_string(method, bucket, key, query_args, headers, int(expire))
        authorization_string = encode(self.access_key_secret, c_string)
        
        if not self.path.endswith('?'):
            self.path += "?"
        self.path += "SNDAAccessKeyId=%s&" % self.access_key_id
        self.path += "Expires=%d&" % expire
        self.path += "Signature=%s" % urllib2.quote(authorization_string)
        
        self.path = SIGNEDURL_HOST + self.path
        
        if self.is_secure:
            self.path = "https://" + self.path
        else:
            self.path = "http://" + self.path
        
        return self.path
    
    def create_signed_put_url(self, bucket='', key='', headers=0, metadata=0, expire=0):
        if headers == 0:
            headers = {}
        if metadata == 0:
            metadata = {}
        query_args = {}
        return self.create_signed_url('PUT', bucket, key, query_args, headers, metadata, expire)

    def create_signed_get_url(self, bucket='', key='', expire=0):
        query_args = headers = metadata = {}
        return self.create_signed_url('GET', bucket, key, query_args, headers, metadata, expire)
    
    def create_signed_head_url(self, bucket='', key='', expire=0):
        query_args = headers = metadata = {}
        return self.create_signed_url('HEAD', bucket, key, query_args, headers, metadata, expire)
    
    def create_signed_delete_url(self, bucket='', key='', expire=0):
        query_args = headers = metadata = {}
        return self.create_signed_url('DELETE', bucket, key, query_args, headers, metadata, expire)
