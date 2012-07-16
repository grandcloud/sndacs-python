'''
Created on Jul 16, 2012
'''

from sndacspylib.snda_cs_config import *
from sndacspylib.snda_cs_model import CompleteMultipartUpload

import sndacspylib.snda_cs.cs_rest as CSRest
import sndacspylib.snda_cs.cs_util as CSUtil
import sndacspylib.snda_cs_genutilities as Util

from xml.dom.minidom import Document

import uuid


# initialize connection
connection = CSRest.SNDAAuthConnection(Config.CSProperties['AccessKey'], Config.CSProperties['SecretKey'], True)

object_name = str(uuid.uuid4())
# initialize object
object = CSUtil.SNDA_Object(connection, "bucket_name", object_name)

#initialize multiupload
object.initiate_multipart_upload()

#abort multiupload
object.abort_multipart_upload(object.init_result.upload_id)

#list parts
list_parts_result = object.list_parts(object.init_result.upload_id)

#upload part from file
part1 = object.upload_part_from_file(object.init_result.upload_id,
                                     '1',
                                     'filepath/file')
part2 = object.upload_part_from_data(object.init_result.upload_id, 
                                     '2', 
                                     'I am No.2.')

#generate complete mulitpart upload xml document
complete_content = CompleteMultipartUpload([part1, part2])
document = Util.object_convert_to_xml(Document(), complete_content)

#complete multiupload
object.complete_multipart_upload(object.init_result.upload_id, document.toxml())

