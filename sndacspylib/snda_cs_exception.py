'''
Created on 2011-7-27
'''

class CSError(Exception):
    def __init__(self, status, reason, method, bucket, key, code=0, message=0, request_id=0, end_point=0):
        self.status = status
        self.reason = reason
        self.method = method
        self.bucket = bucket
        self.key = key
        self.code = code
        self.message = message
        self.request_id = request_id
        self.end_point = end_point

    def __str__(self):
        return 'Error %d(%s). %s on bucket=%s, key=%s\n' % \
               (self.status, self.reason, self.method, self.bucket, self.key)
               
class CSInternalError(CSError):
    def __init__(self, status, reason, method, bucket, key):
        self.status = status
        self.reason = reason
        self.method = method
        self.bucket = bucket
        self.key = key
    def __str__(self):
        return ('Grandcloud storage service encountered an internal error, please try again.' + \
            'Error %d(%s). %s on bucket=%s, key=%s\n') % \
            (self.status, self.reason, self.method, self.bucket, self.key)
               
class CSNotFound (Exception):
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key    = key
        
    def __str__(self):
        return ( 'Object %s + %s not found\n' % (self.bucket, self.key ) )
    
class CSNoSuchUpload(Exception):
    def __init__(self, upload_id):
        self.upload_id = upload_id
        
    def __str__(self):
        return ( 'UploadId %s not found\n' % self.upload_id )
    
class CSInvalidPart(Exception):
    def __init__(self, upload_id):
        self.upload_id = upload_id
    def __str__(self):
        return ( 'UploadId %s request has one or more of the specified parts could not be found\n' % self.upload_id )
    
class CSInvalidPartOrder(Exception):
    def __init__(self, upload_id):
        self.upload_id = upload_id
    def __str__(self):
        return ( 'UploadId %s request list of parts was not in ascending order\n' % self.upload_id )

class CSInvalidAccessKeyId(Exception):
    def __init__(self, access_key_id):
        self.access_key_id = access_key_id
    def __str__(self):
        return ( 'The SNDA Access Key Id you provided does not exist in our records.' )

class InvalidAttribute(Exception):
    def __init__(self, a):
        self.attrib = a
        
    def __str__(self):
        return 'Attribute=%s is Invalid!\n' % (self.attrib)
    
class InvalidParameter(Exception):
    def __init__(self, param):
        self.param = param
    
    def __str__(self):
        return 'Parameter=%s is Invalid!\n' % (self.param)
    
class CSNoSuchFile(Exception):
    def __init__(self, file):
        self.fileName = file

    def __str__(self):
        return ('File %s not found\n' % (self.fileName) )
    
class Failed(Exception):
    def __init__(self):
        pass
        
    def __str__(self):
        return ('Operation failed\n' )
