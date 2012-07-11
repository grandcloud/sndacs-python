'''
Created on 2012-2-21

@author: jiangwenhan
'''
from sndacspylib.snda_cs_exception import *
from sndacspylib.snda_cs_config import Config
import sndacspylib.snda_cs_genutilities as Util
import sndacspylib.snda_cs.cs_util as CSUtil

import uuid

UNIVERSE_BUCKET_NAME = uuid.uuid4().hex

def init():
    conn = CSUtil.CS.SNDAAuthConnection(Config.CSProperties['AccessKey'], \
                                       Config.CSProperties['SecretKey'], ( Config.CSProperties['SecureComm'] == False))
    cloud_storage = CSUtil.SNDA_CS( ConnectionObject  = conn )
        
    return cloud_storage

def pre_create_bucket(cloud_storage):
    
    cloud_storage.add_bucket(UNIVERSE_BUCKET_NAME)
    
def final_delete_bucket(cloud_storage):
    
    cloud_storage.delete_bucket(UNIVERSE_BUCKET_NAME)

def test_bucket(cloud_storage):
       
    # list buckets
    cloud_storage.get_list_of_buckets();
    
    for entry in cloud_storage.ListOfBuckets:
        print '%s\t\t\t%s' % (entry, cloud_storage.ListOfBuckets[entry].creation_date)
        
    # add bucket
    cloud_storage.add_bucket("your-universe-bucket-name")
    
    # delete bucket
    cloud_storage.delete_bucket("your-universe-bucket-name")
    
def test_object(cloud_storage):
    
    pre_create_bucket(cloud_storage)
    
    temp_object = CSUtil.SNDA_Object(cloud_storage.CONN ,UNIVERSE_BUCKET_NAME, "temp_key")
    # upload object
    temp_object.put_object_from_file("C:\Documents and Settings\jiangwenhan\workspace\python\README.md")
    
    # get object meta
    response = temp_object.get_object_info()
    print response.http_response.msg
    
    # download object
    temp_object.get_object_to_file("C:\Documents and Settings\jiangwenhan\workspace\python\README.md.get")
    
    # delete object
    temp_object.delete_object()
    
    final_delete_bucket(cloud_storage)
    
def test_bucket_policy(cloud_storage):
    bucket = CSUtil.SNDA_Bucket(ConnectionObject = cloud_storage.CONN, bucketName = "ptolemaeus_policy_test")
    bucket.set_policy("wo le ge qu")
            
if __name__ == '__main__':
    
    cloud_storage = init()
    test_bucket_policy(cloud_storage)
#    test_bucket(cloud_storage)
#    test_object(cloud_storage)