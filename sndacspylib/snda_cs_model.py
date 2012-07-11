'''
Created on May 31, 2012

@author: jiangwenhan
'''

import uuid

RESOURCE_PREFIX = "srn:snda:storage:::"

class Effects(object):
    """
    The Effect is a required element that indicates 
    whether you want the statement to result in an allow or an explicit deny
    
    @type Allow: string
    @ivar Allow: allow
    @type Deny: string
    @ivar Deny: deny
    """
    
    Allow = "Allow"
    Deny = "Deny"

class Actions(object):
    """
    SNDA cloud storage actions can be referred in a policy
    
    @type AllActions: string
    @ivar AllActions: all actions
    @type GetObject: string
    @ivar GetObject: get object action
    @type PutObject: string
    @ivar PutObject: put object action
    @type DeleteObject: string
    @ivar DeleteObject: delete object action
    @type ListMultipartUploadParts: string
    @ivar ListMultipartUploadParts: list multipart upload parts action
    @type AbortMultipartUpload: string
    @ivar AbortMultipartUpload: abort multipart upload action
    
    @type CreateBucket: string
    @ivar CreateBucket: create bucket action
    @type DeleteBucket: string
    @ivar DeleteBucket: delete bucket action
    @type ListBucket: string
    @ivar ListBucket: list bucket action
    @type ListAllMyBuckets: string
    @ivar ListAllMyBuckets: list all my bucket action
    @type ListBucketMultipartUploads: string
    @ivar ListBucketMultipartUploads: list bucket multipart upload action
    
    @type GetBucketLocation: string
    @ivar GetBucketLocation: get bucket location action
    @type GetBucketPolicy: string
    @ivar GetBucketPolicy: get bucket policy action
    @type PutBucketPolicy: string
    @ivar PutBucketPolicy: put bucket policy action
    @type GetBucketLogging: string
    @ivar GetBucketLogging: get bucket logging action
    @type PutBucketLogging: string
    @ivar PutBucketLogging: put bucket logging action
    """
    AllActions = "*"
    #Actions Related to Objects
    GetObject = "storage:GetObject"
#    GetObjectVersion = "storage:GetObjectVersion"
    PutObject = "storage:PutObject"
#    GetObjectAcl = "storage:GetObjectAcl"
#    GetObjectVersionAcl = "storage:GetObjectVersionAcl"
#    PutObjectAcl = "storage:PutObjectAcl"
#    PutObjectVersionAcl = "storage:PutObjectVersionAcl"
    DeleteObject = "storage:DeleteObject"
#    DeleteObjectVersion = "storage:DeleteObjectVersion"
    ListMultipartUploadParts = "storage:ListMultipartUploadParts"
    AbortMultipartUpload = "storage:AbortMultipartUpload"
           
    #Actions Related to Buckets
    CreateBucket = "storage:CreateBucket"
    DeleteBucket = "storage:DeleteBucket"
    ListBucket = "storage:ListBucket"
#    ListBucketVersions = "storage:ListBucketVersions"
    ListAllMyBuckets = "storage:ListAllMyBuckets"
    ListBucketMultipartUploads = "storage:ListBucketMultipartUploads"
           
    #Actions Related to Bucket Sub-Resources
#    GetBucketAcl = "storage:GetBucketAcl"
#    PutBucketAcl = "storage:PutBucketAcl"
#    GetBucketVersioning = "storage:GetBucketVersioning"
#    PutBucketVersioning = "storage:PutBucketVersioning"
#    GetBucketRequesterPays = "storage:GetBucketRequesterPays"
#    PutBucketRequesterPays = "storage:PutBucketRequesterPays"
    GetBucketLocation = "storage:GetBucketLocation"
    GetBucketPolicy = "storage:GetBucketPolicy"
    PutBucketPolicy = "storage:PutBucketPolicy"
#    GetBucketNotification = "storage:GetBucketNotification"
#    PutBucketNotification = "storage:PutBucketNotification"
    GetBucketLogging = "storage:GetBucketLogging"
    PutBucketLogging = "storage:PutBucketLogging"
#    GetLifecycleConfiguration = "storage:GetLifecycleConfiguration"
#    PutLifecycleConfiguration = "storage:PutLifecycleConfiguration"


class PolicyKeys(object):
    '''
    abstraction of policy keys
    '''

    
class AvailableKeys(PolicyKeys):
    """
    SNDA provides a set of common keys supported by storage services 
    that adopt the access policy language for access control
    
    @type CurrentTime: string
    @ivar CurrentTime: For date/time conditions
    @type SecureTransport: string
    @ivar SecureTransport: Boolean representing whether the request was sent using SSL
    @type SourceIp: string
    @ivar SourceIp: The requester's IP address, for use with IP address conditions
    @type UserAgent: string
    @ivar UserAgent: Information about the requester's client application, for use with string conditions
    @type EpochTime: string
    @ivar EpochTime: The date in epoch or UNIX time, for use with date/time conditions
    """
    CurrentTime = "snda:CurrentTime"
    SecureTransport = "snda:SecureTransport"
    SourceIp = "snda:SourceIp"
    UserAgent = "snda:UserAgent"
    EpochTime = "snda:EpochTime"
    Referer = "snda:Referer"

    
class BucketAndObjectKeys(PolicyKeys):
    """
    Keys related to objects that can be used in policies
    
    @type x_snda_copy_source: string
    @ivar x_snda_copy_source: The header that specifies the name of the source bucket and key name of the source object, separated by a slash (/)
    @type x_snda_metadata_directive: string
    @ivar x_snda_metadata_directive: The header that specifies whether the metadata is copied from the source object or replaced with metadata provided in the request. If copied, the metadata remains unchanged. Otherwise, all original metadata is replaced by the metadata you specify
    @type LocationConstraint: string
    @ivar LocationConstraint: Specifies the Region where the bucket will be created
    @type prefix: string
    @ivar prefix: Limits the response to objects that begin with the specified prefix
    @type delimiter: string
    @ivar delimiter: The character you use to group objects
    @type max_keys: string
    @ivar max_keys: The number of objects to return from the call. The maximum allowed value (and default) is 1000
    """
    
    #PutObject && PutObjectAcl && #PutObjectVersionAcl
    #CreateBucket && PutBucketAcl
#    x_snda_acl = "storage:x-snda-acl"
    
    #PutObject
    x_snda_copy_source = "storage:x-snda-copy-source"
    x_snda_metadata_directive = "storage:x-snda-metadata-directive"
    
    #GetObjectVersion && GetObjectVersionAcl && PutObjectVersionAcl && DeleteObjectVersion
#    VersionId = "storage:VersionId"
    
    #CreateBucket
    LocationConstraint = "storage:LocationConstraint"
    
    #ListBucket && ListBucketVersions
    prefix = "storage:prefix"
    delimiter = "storage:delimiter"
    max_keys = "storage:max-keys"
    

class ConditionTypes(object):
    """
    General types of conditions you can specify
    
    @type StringEquals: string
    @ivar StringEquals: Strict matching
    @type StringNotEquals: string
    @ivar StringNotEquals: Strict negated matching
    @type StringEqualsIgnoreCase: string
    @ivar StringEqualsIgnoreCase: Strict matching, ignoring case
    @type StringNotEqualsIgnoreCase: string
    @ivar StringNotEqualsIgnoreCase: Strict negated matching, ignoring case
    @type StringLike: string
    @ivar StringLike: Loose case-sensitive matching. The values can include a multi-character match wildcard (*) or a single-character match wildcard (?) anywhere in the string
    @type StringNotLike: string
    @ivar StringNotLike: Negated loose case-insensitive matching. The values can include a multi-character match wildcard (*) or a single-character match wildcard (?) anywhere in the string
    
    @type NumericEquals: string
    @ivar NumericEquals: Strict matching
    @type NumericNotEquals: string
    @ivar NumericNotEquals: Strict negated matching
    @type NumericLessThan: string
    @ivar NumericLessThan: "Less than" matching
    @type NumericLessThanEquals: string
    @ivar NumericLessThanEquals: "Less than or equals" matching
    @type NumericGreaterThan: string
    @ivar NumericGreaterThan: "Greater than" matching
    @type NumericGreaterThanEquals: string
    @ivar NumericGreaterThanEquals: "Greater than or equals" matching
    
    @type DateEquals: string
    @ivar DateEquals: Strict matching
    @type DateNotEquals: string
    @ivar DateNotEquals: Strict negated matching
    @type DateLessThan: string
    @ivar DateLessThan: A point in time at which a key stops taking effect
    @type DateLessThanEquals: string
    @ivar DateLessThanEquals: A point in time at which a key stops taking effect
    @type DateGreaterThan: string
    @ivar DateGreaterThan: A point in time at which a key starts taking effect
    @type DateGreaterThanEquals: string
    @ivar DateGreaterThanEquals: A point in time at which a key starts taking effect
    
    @type Bool: string
    @ivar Bool: Strict Boolean matching
    
    @type IpAddress: string
    @ivar IpAddress: Approval based on the IP address or range
    @type NotIpAddress: string
    @ivar NotIpAddress: Denial based on the IP address or range
    """
    #String Conditions
    StringEquals = "StringEquals"
    StringNotEquals = "StringNotEquals"
    StringEqualsIgnoreCase = "StringEqualsIgnoreCase"
    StringNotEqualsIgnoreCase = "StringNotEqualsIgnoreCase"
    StringLike = "StringLike"
    StringNotLike = "StringNotLike"
    
    #Numeric Conditions
    NumericEquals = "NumericEquals"
    NumericNotEquals = "NumericNotEquals"
    NumericLessThan = "NumericLessThan"
    NumericLessThanEquals = "NumericLessThanEquals"
    NumericGreaterThan = "NumericGreaterThan"
    NumericGreaterThanEquals = "NumericGreaterThanEquals"
    
    #Date Conditions
    DateEquals = "DateEquals"
    DateNotEquals = "DateNotEquals"
    DateLessThan = "DateLessThan"
    DateLessThanEquals = "DateLessThanEquals"
    DateGreaterThan = "DateGreaterThan"
    DateGreaterThanEquals = "DateGreaterThanEquals"
    
    #Boolean Conditions
    Bool = "Bool"
    
    #IP Address
    IpAddress = "IpAddress"
    NotIpAddress = "NotIpAddress "
    

class ConditionItem(object):
    
    def __init__(self, ConditionType, ConditionValues={}):
        self.ConditionType = ConditionType
        self.ConditionValues = ConditionValues

class PolicyStatement(object):
    
    def __init__(self, Sid, Effect, Principal, Action=[], Resource="", Condition={}):
        if Sid is not None:
            self.Sid = Sid
        self.Effect = Effect
        self.Principal = Principal
        self.Action = Action
#        self.Resource = Resource
        self.set_resource(Resource)
        self.Condition = Condition
        
    def sid_regenerate(self):
        self.Sid = str(uuid.uuid4())
        
    def effect_allow(self):
        self.Effect = Effects.Allow
        
    def effect_deny(self):
        self.Effect = Effects.Deny
        
    def insert_action(self, action):
        #TODO check existence of action
        self.Action.append(action)
        
    def set_resource(self, resource):
        if resource.startswith(RESOURCE_PREFIX):
            self.Resource = resource
        else:
            self.Resource = "%s%s" % (RESOURCE_PREFIX, resource)
        
    def insert_condition(self, condition_type, policy_key, policy_value):
        if self.Condition.has_key(condition_type):
            if self.Condition[condition_type].has_key(policy_key):
                self.Condition[condition_type][policy_key].append(policy_value)
            else:
                self.Condition[condition_type][policy_key] = [policy_value]
        else:
            self.Condition[condition_type] = {policy_key : [policy_value]}
        
    def toDict(self):
        gen_dict = {}
        if self.Effect is not None:
            gen_dict["Effect"] = self.Effect
        if self.Sid is not None:
            gen_dict["Sid"] = self.Sid
        if self.Principal is not None:
            gen_dict["Principal"] = self.Principal
        if self.Action is not None:
            gen_dict["Action"] = self.Action
        if self.Resource is not None:
            gen_dict["Resource"] = self.Resource
        if self.Condition is not None:
            gen_dict["Condition"] = self.Condition
        return gen_dict
    
            
class BucketPolicy(object):

    def __init__(self, Id, Version, Statement=[]):
        self.Id = Id
        self.Version = Version
        self.Statement = Statement
        
    def toDict(self):
        gen_dict = {}
        if self.Version is not None:
            gen_dict["Version"] = self.Version
        if self.Id is not None:
            gen_dict["Id"] = self.Id
        if self.Statement is not None:
            gen_dict["Statement"] = []
            for statement in self.Statement:
                gen_dict["Statement"].append(statement.toDict())
        return gen_dict   
    
class Part(object):
    
    def __init__(self, part_number='', etag=''):
        self.PartNumber = part_number
        self.ETag = etag     
        
class CompleteMultipartUpload(object):
    
    def __init__(self, parts=[]):
        self.Part = parts