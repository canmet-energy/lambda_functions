# *******************************************************************************
# Copyright (c) 2008-2019, Natural Resources Canada
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# (1) Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# (2) Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# (3) Neither the name of the copyright holder nor the names of any contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission from the respective party.
#
# (4) Other than as required in clauses (1) and (2), distributions in any form
# of modifications or other derivative works may not use the "BTAP"
# trademark, or any other confusingly similar designation without
# specific prior written permission from Natural Resources Canada.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER(S) AND ANY CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER(S), ANY CONTRIBUTORS, THE
# CANADIAN FEDERAL GOVERNMENT, OR NATURAL RESOURCES CANADA, NOR ANY OF
# THEIR EMPLOYEES, BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
# OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# *******************************************************************************

require 'json'
require 'aws-sdk-s3'

def lambda_handler(event:, context:)
  osa_id = event["osa_id"]
  bucket_name = event["bucket_name"]
  analysis_name = event["analysis_name"]
  aws_region = "us-east-1"
  unless event["region"].nil?
    aws_region = event["region"]
  end
  folder_name = analysis_name + "_" + osa_id
  object_names = search_objects(osa_id: folder_name, bucket_name: bucket_name, region: aws_region)
  { statusCode: 200, body: JSON.generate(object_names) }
end

def search_objects(osa_id:, bucket_name:, region:)
  s3 = Aws::S3::Resource.new(region: region)
  bucket = s3.bucket(bucket_name)
  #Go through all of the objects in the s3 bucket searching for the qaqc.json and error.json objects related the current
  #analysis.
  object_names = []
  bucket.objects.each do |bucket_info|
    unless (/#{osa_id}/ =~ bucket_info.key.to_s).nil?
      #Remove the / characters with _ to avoid regex problems
      replacekey = bucket_info.key.to_s.gsub(/\//, '_')
      #Search for objects with the current analysis id that have .zip in them, then extract the qaqc and error data
      #and collate those into a qaqc_col array of hashes and an error col array of hashes.  Ultimately thes end up in an
      #s3 bucket on aws.
      unless (/.zip/ =~ replacekey.to_s).nil?
        #If you find an osw.zip file try downloading it and adding the information to the error_col array of hashes.
        object_names << bucket_info.key.to_s
      end
    end
  end
  return object_names
end