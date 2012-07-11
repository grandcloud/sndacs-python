= sndacs

sndacs library provides access to {SNDA Cloud Storage}[http://www.grandcloud.cn/product/ecs].

== Installation

    python setup.py install

== Usage

=== Initialize the connnection with authentication

    import sndacspylib.snda_cs.cs_util as CSUtil
    from sndacspylib.snda_cs_config import Config
    connection = CSUtil.CS.SNDAAuthConnection(Config.CSProperties['AccessKey'], \
                                              Config.CSProperties['SecretKey'], \
                                              (Config.CSProperties['SecureComm']==False))

=== Initialize the service

    import sndacspylib.snda_cs.cs_util as CSUtil
    service = CSUtil.SNDA_CS(ConnectionObject = connection)

=== List buckets

    bucket_list = service.get_list_of_buckets()

=== Add bucket

    service.add_bucket("bucket_name", "huabei-1")

=== Delete bucket

    service.delete_bucket("bucket_name")

=== Initialize the bucket

    import sndacspylib.snda_cs.cs_util as CSUtil
    bucket = CSUtil.SNDA_Bucket(ConnectionObject = connection, bucketName = "bucket_name")

=== List objects

    object_list = bucket.get_list_of_keys_in_bucket(prefixDir = 'abc', delimiter='/')

=== Generate bucket policy string

    from sndacspylib.snda_cs_model import *
    effect = Effects.Allow
    actions = Actions.AllActions
    resources = "*"
    conditions = {ConditionTypes.Bool: {AvailableKeys.SecureTransport: True}, \
                  ConditionTypes.IpAddress: {AvailableKeys.SourceIp: "192.168.0.24"}}
    statement = PolicyStatement(Sid = None, \
                                Effect = effect, \
                                Principal = None, \
                                Action = actions, \
                                Resource = resources, \
                                Condition = conditions)
    Statement.sid_regenerate()
    policy = BucketPolicy(Id = "your_id", Version = None, Statement = [statement])
    import json
    policy_xml = json.dumps(policy.toDict())

=== Set bucket policy

    bucket.set_policy(policy_xml)

=== Get bucket policy

    bucket_policy_string = bucket.get_policy()

=== Delete bucket policy

    bucket.delete_policy()

=== Initialize the object

    import sndacspylib.snda_cs.cs_util as CSUtil
    object = CSUtil.SNDA_Object(ConnectionObject = connection, "bucket_name", "object_name")

=== Upload object from file

    object.put_object_from_file("filepath/file")

=== Get object infomations

    infos = object.get_object_info()

=== Download object to file

    object.get_object_to_file("filepath/file")

=== Delete object

    object.delete_object()

== Copyright

Copyright (c) 2012 grandcloud.cn.
All rights reserved.
