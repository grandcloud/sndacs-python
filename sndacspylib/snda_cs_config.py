'''
Created on 2011-7-27

'''
from logging import NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL
from optparse import OptionParser
import logging
import logging.config
import sndacspylib.snda_cs_genutilities as Util
import sys



ASCII_LINEFEED = u'\n'
EQUAL = u'='

LOGCONFIG_FILENAME = 'sndacspylib/config/cs_logconfig.ini'
CSCONFIG_FILENAME  = 'sndacspylib/config/cs.properties'

class CSConfig(object):
    
    def __init__(self):
        self.CSProperties = {}
        self.get_configs()
        
    def process_line(self, line):
        """
        This function processes a line from a configuration file
        Lines that are not blank and do not start with a "#" are parsed
        for a name:value pair
        """
        lines = []
        
        result = line.rstrip().rstrip(ASCII_LINEFEED)
        
        if result == '' or result[:1] == '#':
            pass
        else:
            lines.append(result)
            #kvpair = result.split(EQUAL)
            #self.CSProperties[kvpair[0]] = kvpair[1]
            split_pos = result.find(EQUAL)
            self.CSProperties[result[:split_pos]] = result[split_pos+1:]
        return
                
    def get_configs(self):
        """
        This function set's up a parser, processes command line arguments
        and reads the configuration from the configuration file specified 
        as a cmd line argument
        """
        
        log_level = [NOTSET, DEBUG, INFO, \
                     WARNING, ERROR, CRITICAL]
        
        parser = OptionParser(version="%prog 0.1", usage="""
        This function set's up a parser, processes command line arguments
        and reads the configuration from the configuration file specified 
        as a cmd line argument
        """)
        
        parser.add_option('-i', '--input', action='store', type='string', dest='inputfile', default='', \
                          help='input file. Default is cs.properties in PYTHONPATH')
        parser.add_option('-c', '--logconfig', action='store', type='string', dest='logconfigfile', default='', \
                          help='log configuration file. Default is cs_logconfig.ini in PYTHONPATH')
        
        (ConfigOptions, args) = parser.parse_args()
        
        #input configuration file
        input = None
        if ConfigOptions.inputfile:
            input = open(ConfigOptions.inputfile, 'r')
        else:
            input_file_name = Util.find_file(CSCONFIG_FILENAME)
            if input_file_name == None:
                print 'Could not find input configuration file %s in PYTHONPATH' % CSCONFIG_FILENAME
                sys.exit(0)
            else:
                input = open(input_file_name, 'r')
                
        #input log configuration file
        log_config_file = Util.find_file(LOGCONFIG_FILENAME)
        if log_config_file == None:
            print 'Could not find log configuration file %s in PYTHONPATH' % LOGCONFIG_FILENAME
            sys.exit(0)
        else:
            logging.config.fileConfig(log_config_file)

        #log command input
        logging.debug('options:%s' % ConfigOptions)
        logging.debug('args:%s' % args)

        #process configuration file
        nLine = 0
        for line in input:
            self.process_line(line)
            nLine += 1
        
        #log properties in configuration file
        logging.debug('CSProperties: %s' % self.CSProperties)
        
        input.close()
        return
        
Config = CSConfig()
