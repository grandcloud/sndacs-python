'''
Created on Jun 26, 2012

'''

import sndacspylib.snda_cs_genutilities as Util

import sys


MIME_TYPE_FILE = 'sndacspylib/config/mime.types'

class MIMEType(object):
    '''
    MIME map of the file extensions
    '''
    def __init__(self):
        self.types = {}
        self._init_type_()
        
    def _init_type_(self):
        
        input_file_name = Util.find_file(MIME_TYPE_FILE)
        if input_file_name == None:
            sys.stderr.write('Could not find mime type file %s in PYTHONPATH' % MIME_TYPE_FILE)
            return
        else:
            file = open(input_file_name, 'r')
        line = file.readline()
        while line:
            if line.startswith('#'):
                line = file.readline()
                continue
            
            parts = line.strip().split()
            line = file.readline()
            
            if len(parts) < 2:
                continue
            
            value = parts[0]
            for i in xrange(1, len(parts)):
                self.types[parts[i]] = value
                
    def get_type(self, extension):
        if not self.types.has_key(extension):
            return 'binary/octet-stream'
        else:
            return self.types[extension]
        
MIME_TYPES = MIMEType()