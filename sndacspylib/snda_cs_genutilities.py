'''
Created on 2011-7-27

'''

from hashlib import md5
from xml.dom.minidom import Document
from sndacspylib.snda_cs_exception import InvalidAttribute
import os
import sys
import types


CS_TIME_FORMAT = '%a %b %d %H:%M:%S %Y'
CHUNK_SIZE = 65536
DELIMITER = u'/'

class my_dir:
    def __init__ (self, access_key, name, depth, parent=u''):
        self.access_key = access_key
        self.name = name
        self.depth = depth
        self.parent_name = parent
        self.children = {}
        self.__name__ = my_dir.__name__

    def add_child( self, child):
        """
        Add child node
        
        @type child: my_dir or my_file
        @param child: directory node or file node to be added
        """
        if ( (child.__name__ != 'my_dir') and (child.__name__ != 'my_file') ):
            raise InvalidAttribute(child)
        self.children[child.name] = child

    def get_child(self, key):
        """
        Get child node
        
        @type key: string
        @param key: node name
        
        @rtype: my_dir or my_file
        @return: my_dir node or my_file node, if no such object, then return None
        """
        if self.children.has_key( key ):
            return self.children[key]

        return None

    def delete_child(self, key):
        """
        Delete child node
        
        @type key: string
        @param key: node name
        """
        if self.children.has_key( key ):
            del self.children[key]

class my_file:
    def __init__ (self, access_key, name, depth, size, owner, last_modified, hash, parent= u''):
        self.access_key = access_key
        self.name = name
        self.depth = depth
        self.size = size
        self.owner = owner
        self.last_modified = last_modified
        self.hash = hash    
        self.parent_name = parent
        self.__name__ = my_file.__name__

def find_file(file):
    """
    Finds a file in PYTHONPATH.
    Returns the full path-name of file, if found, else returns None
    """
    
    for dirname in sys.path:
        possible_file_name = os.path.join(dirname, file)
        if (os.path.isfile(possible_file_name)):
            return possible_file_name
        
    return None

def print_list(list, fPrintDetails=False):
    """
    Takes list of entries and pretty prints it out with indices
    Returns the number of entries printed
    """
    
    index = 0
    for entry in list:
        if (fPrintDetails):
            print '[%d] %s \t %s' % (index, entry.name,  entry.creation_date)
        else:
            print '[%d] %s' % (index, entry.name)        
        index = index + 1

    return index - 1

def print_dir_list(list, fPrintDetails=False):
    """
    Takes list of entries and pretty prints it out with indices
    Returns the number of entries printed
    """
    index = 0
    for entry in list:
        if (fPrintDetails):
            print '[%d] %s \t %s \t %s' % (index, entry['name'],  entry['last_modified'], entry['owner'])
        else:
            print '[%d] %s' % (index, entry['name'])        
        index = index + 1

    return ( index - 1)

def get_string_input ( prompt ):
    """
    Get something input
    
    @type prompt: string
    @param prompt: inputs
    """
    while True:
        input = raw_input(prompt)
        if (input != ''):
            break
        
    return (input)

def get_hash_from_filename ( fileName ):
    """
    Calculate md5 of file
    
    @type fileName: string
    @param fileName: path of file to calculate
    
    @rtype: bytes
    @return: hex bytes of the calculating result
    """
    fp = 0
    try:
        hash = md5.new( )
        fp = open (fileName, 'rb')

        while True:
            bytes = fp.read (CHUNK_SIZE)
            if not bytes:
                break

            hash.update( bytes)

    except IOError:
        print '%s: IOError on file=%s' % (__name__, fileName )
    except Exception:
        print '%s: Unhandled exception' % __name__
    finally:
        if fp: fp.close( )
        return ( hash.hexdigest( ) )
    
def get_hash_from_file ( fp ):
    """
    Calculate md5 of file
    
    @type fp: file pointer
    @param fp: path of file to calculate
    
    @rtype: bytes
    @return: hex bytes of the calculating result
    """
    try:
        while True:
            bytes = fp.read(CHUNK_SIZE)
            if not bytes:
                break
            hash.update (bytes )

    except IOError:
        print '%s: IOError on file=%s' % (__name__, fp.name )
    except Exception:
        print '%s: Unhandled exception' % __name__
    finally:
        return ( hash.hexdigest( ) )

def get_input ( config, low_range = 0, high_range = 99):
    action = None
    value = None
    while True:
        prompt = 'Enter[%d-%d]. %s-->' % (low_range, high_range, config)
        userInput = raw_input( prompt )
        for entry in config:

            if (userInput.lower( ) == entry[0]):
                action = entry[1]
                break
        if action:
            break
        if ( userInput.isdigit( ) == True ):
            if ( low_range <= int(userInput) <= high_range ):
                value = int(userInput)
                break        
            else:
                print 'ERROR! Your entered:%s' % userInput        
        else:
            print 'ERROR! Your entered:%s' % userInput            

    return (action, value)

def get_digit_input( low_range = 0, high_range = 99 ):
    """
    Get digit input
    
    @type low_range: number
    @param low_range: low range
    @type high_range: number
    @param high_range: high range
    """
    fDone = False
    while not fDone:
        prompt = 'Enter[' + str(low_range) + ':' + str(high_range) + ']-->'
        userInput = raw_input( prompt )
        if ( userInput.isdigit( ) == True ):
            if ( int(userInput) > high_range) or (int(userInput) < low_range ):
                print 'ERROR! Your entered:%s' % userInput
            else:
                fDone = True
        else:
            print 'ERROR! Your entered:%s' % userInput            

    return ( int(userInput) )

def delete_directory( path ):
    """
    Take a "root" path and recursiverly deletes everything under it
    Equivalent of am "rm -rf"
    
    @type path: string
    @param path: path of directory
    """
    if (os.path.exists(path) == True):
        for root, dirs, files in os.walk ( path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name) )
                          
            for name in dirs:
                os.rmdir(os.path.join(root, name) )
            
        os.rmdir(path)
        
def make_test_file( filename, file_size = 0):
    """
    Creates a test file of specified name and size whose content is random
    """
    import random

    if (file_size == 0):
        file_size = random.randint(100, 100000)
    
    len = 0
    fp = open(filename, 'wb')
    while len < file_size:
        fp.write(random.choice('0123456789abcdefghijklmnopqrstuvwxyz') )
        len += 1
    fp.flush()
    fp.close( )            
    return

def object_convert_to_xml(document, inst, element=None):
    if not element:
        root = document.createElement(inst.__class__.__name__)
    for attr in inst.__dict__.keys():
        if attr[0] == '_':
            continue
        value = getattr(inst, attr)
        if type(value) == types.ListType:
            for item in value:
                list_element = document.createElement(attr)
                object_convert_to_xml(document, item, list_element)
                root.appendChild(list_element)
        elif type(value) == types.InstanceType:
            object_convert_to_xml(document, value)
        else:
            element_node = document.createElement(attr)
            text_node = document.createTextNode(str(value))
            element_node.appendChild(text_node)
            element.appendChild(element_node)
    if not element:
        document.appendChild(root)
        
def generate_bucket_location_xml(location):
    document = Document()
    root_element = document.createElement("CreateBucketConfiguration")
    location_element = document.createElement("LocationConstraint")
    location_node = document.createTextNode(location)
    location_element.appendChild(location_node)
    root_element.appendChild(location_element)
    document.appendChild(root_element)
    
    return document.toxml()
