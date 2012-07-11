'''
Created on 2011-8-1

@author: jiangwenhan
'''

try:

    from setuptools import setup

except ImportError:

    from distutils.core import setup



__version__ = '0.1'



setup(name = "sndacspylib",

      version = __version__,

      description = "SNDA ECS Python Library",

      long_description="Python Library to SNDA Cloud Storage Web Services",

      author_email = "jiangwenhan@snda.com",

      packages = [ 'sndacspylib', 'sndacspylib.snda_cs', 'sndacspylib.test'],

      package_dir={'sndacspylib': 'sndacspylib'},

      package_data={'sndacspylib': ['config/*', 'docs/*']},

      platforms = 'Posix; Windows',

      )