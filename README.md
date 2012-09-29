# sndacs

sndacs library provides access to [SNDA Cloud Storage](http://www.grandcloud.cn/product/ecs).

## Installation

    python setup.py install

## Usage

### Initialize the connnection with authentication

    import sndacspylib.snda_cs.cs_util as CSUtil
    from sndacspylib.snda_cs_config import Config
    connection = CSUtil.CS.SNDAAuthConnection(Config.CSProperties['AccessKey'], 
                                              Config.CSProperties['SecretKey'], 
                                              (Config.CSProperties['SecureComm']==False))

### Initialize the service

    import sndacspylib.snda_cs.cs_util as CSUtil
    service = CSUtil.SNDA_CS(ConnectionObject = connection)

### List buckets

    bucket_list = service.get_list_of_buckets()
    for bucket in bucket_list:
        '''
        bucket instance has following attributes
        '''
        print bucket.name, bucket.creation_date, bucket.location

### Add bucket

    service.add_bucket("bucket_name", "huabei-1")

### Delete bucket

    service.delete_bucket("bucket_name")

### Initialize the bucket

    import sndacspylib.snda_cs.cs_util as CSUtil
    bucket = CSUtil.SNDA_Bucket(ConnectionObject = connection, bucketName = "bucket_name")

### List objects

    object_list = bucket.get_list_of_keys_in_bucket(prefixDir = 'abc', delimiter='/')
    for object in object_list:
        '''
        object instance has following attributes
        '''
    	print object.name, object.last_modified, \
    	      object.etag, object.size, \
    	      object.storage_class, object.owner

### Generate bucket policy string

    from sndacspylib.snda_cs_model import *
    effect = Effects.Allow
    actions = Actions.AllActions
    resources = "*"
    conditions = {ConditionTypes.Bool: {AvailableKeys.SecureTransport: True}, \
                  ConditionTypes.IpAddress: {AvailableKeys.SourceIp: "192.168.0.24"}}
    statement = PolicyStatement(Sid = None, 
                                Effect = effect, 
                                Principal = None, 
                                Action = actions, 
                                Resource = resources, 
                                Condition = conditions)
    Statement.sid_regenerate()
    policy = BucketPolicy(Id = "your_id", Version = None, Statement = [statement])
    import json
    policy_xml = json.dumps(policy.toDict())

### Set bucket policy

    bucket.set_policy(policy_xml)

### Catch invalid parameter error

    try:
        bucket.set_policy(None)
    except InvalidParameter, e:
        '''
        Do some other things you want to do
        '''
    	pass

### Get bucket policy

    bucket_policy_string = bucket.get_policy()

### Delete bucket policy

    bucket.delete_policy()

### List multipart uploads in bucket

    list_result = bucket.list_multipart_uploads(key_marker='key-marker',
                                                prefixDir='prefix',
                                                delimiter='delimiter',
                                                upload_id_marker='upload-id-marker')
    for upload in list_result.uploads:
        print upload.key, upload.initiated
    for common_prefix in list_result.common_prefixes:
        print common_prefix.prefix

### Initialize the object

    import sndacspylib.snda_cs.cs_util as CSUtil
    object = CSUtil.SNDA_Object(ConnectionObject = connection, "bucket_name", "object_name")

### Upload object from file

    object.put_object_from_file("filepath/file")

### Upload object as reduced redundency storage class

    object.put_object_from_file("filepath/file", {'x-snda-storage-class' : 'REDUCED_REDUNDANCY'})

### Get object infomations

    infos = object.get_object_info()

### Download object to file

    object.get_object_to_file("filepath/file")

### Delete object

    object.delete_object()

### Catch response error exception

    try:
        print bucket.get_policy()
    except CSError, e:
    	print e.status, e.reason
    	print e.code, e.message, e.request_id

### initialize multiupload

    object.initiate_multipart_upload()

### abort multiupload

    object.abort_multipart_upload(object.init_result.upload_id)

### list parts

    list_parts_result = object.list_parts(object.init_result.upload_id)

### upload part from file

    part1 = object.upload_part_from_file(object.init_result.upload_id,
                                         '1',
                                         'filepath/file')

### upload part from data

    part2 = object.upload_part_from_data(object.init_result.upload_id,
                                         '2',
                                         'I am No.2.')

### generate complete mulitpart upload xml document

    from xml.dom.minidom import Document
    complete_content = CompleteMultipartUpload([part1, part2])
    document = Util.object_convert_to_xml(Document(), complete_content)

### complete multiupload

    object.complete_multipart_upload(object.init_result.upload_id, document.toxml())

### create signed url

    signed_put_url = connection.create_signed_put_url(bucket='testBucket', key='testKey', headers={}, metadata={}, expire=3000)
    signed_get_url = connection.create_signed_get_url(bucket='testBucket', key='testKey', expire=3000)
    signed_head_url = connection.create_signed_head_url(bucket='testBucket', key='testKey', expire=3000)
    signed_delete_url = connection.create_signed_delete_url(bucket='testBucket', key='testKey', expire=3000)


## Copyright

Copyright (c) 2012 grandcloud.cn.
All rights reserved.
