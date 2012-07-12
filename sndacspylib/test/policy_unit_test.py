'''
Created on Jul 12, 2012
'''

from sndacspylib.snda_cs_config import *
from sndacspylib.snda_cs_model import Effects, Actions, PolicyStatement,\
    BucketPolicy

import sndacspylib.snda_cs.cs_rest as CSRest
import sndacspylib.snda_cs.cs_util as CSUtil

import uuid
import json


# initialize connection
connection = CSRest.SNDAAuthConnection(Config.CSProperties['AccessKey'], Config.CSProperties['SecretKey'], True)

# initialize service
service = CSUtil.SNDA_CS(ConnectionObject = connection)

bucket_name = str(uuid.uuid4())
# add bucket
service.add_bucket(bucket_name)

# generate policy
effect = Effects.Allow
principal = None
actions = Actions.GetObject
resources = "%s/*" % bucket_name
statement = PolicyStatement(Sid=None, Effect=effect, Principal=None,
                            Action=actions, Resource=resources,
                            Condition=None)
statement.sid_regenerate()
policy = BucketPolicy(Id="1", Version=None, Statement=[statement])
policy_xml = json.dumps(policy.toDict())

# initialize bucket
bucket = CSUtil.SNDA_Bucket(connection, bucket_name)

# set policy
bucket.set_policy(policy_xml)

# get policy
policy_xml_ret = bucket.get_policy()

print policy_xml
print policy_xml_ret

# delete policy
bucket.delete_policy()

# delete bucket
service.delete_bucket(bucket_name)