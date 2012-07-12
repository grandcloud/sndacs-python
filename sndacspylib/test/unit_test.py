'''
Created on Jul 12, 2012
'''

from sndacspylib.snda_cs_config import *

import sndacspylib.snda_cs.cs_rest as CSRest
import sndacspylib.snda_cs.cs_util as CSUtil

import uuid


# initialize connection
connection = CSRest.SNDAAuthConnection(Config.CSProperties['AccessKey'], Config.CSProperties['SecretKey'], True)

# initialize service
service = CSUtil.SNDA_CS(ConnectionObject = connection)

# list buckets
bucket_list = service.get_list_of_buckets()

for item in bucket_list:
    print bucket_list[item]
    
bucket_name = str(uuid.uuid4())
# add bucket
service.add_bucket(bucket_name)

object_name = str(uuid.uuid4())
# initialize object
object = CSUtil.SNDA_Object(connection, bucket_name, object_name)

# add object
object.put_object_from_file("filepath/file")

# head object
infos = object.get_object_info()
print infos.metadata
print infos.size
print infos.last_modified

# get object
object.get_object_to_file("filepath/file.bak")

import commands
md5sum1 = commands.getoutput("md5sum filepath/file").split()[0]
md5sum2 = commands.getoutput("md5sum filepath/file.bak").split()[0]
print md5sum1
print md5sum2

# initialize bucket
bucket = CSUtil.SNDA_Bucket(connection, bucket_name)

# list object
object_list = bucket.get_list_of_keys_in_bucket("", "")
for item in object_list:
    print item

# delete object
object.delete_object()

# delete bucket
service.delete_bucket(bucket_name)
