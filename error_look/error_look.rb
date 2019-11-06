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

require 'rubygems'
require 'aws-sdk-s3'
require 'fileutils'
require 'zip'
require 'json'

def handler(event:, context:)
  osa_id = event["osa_id"]
  bucket_name = event["bucket_name"]
  aws_region = event["region"]
  analysis_json = {
      analysis_id: event["analysis_json"]["analysis_id"],
      analysis_name: event["analysis_json"]["analysis_name"]
  }
  object_key_file = analysis_json[:analysis_name] + '_' + osa_id + '/' + 'datapoint_ids.json'
  object_keys = get_s3_stream(file_id: object_key_file, bucket_name: bucket_name, region: aws_region)
  response = process_analysis(osa_id: osa_id, analysis_json: analysis_json, bucket_name: bucket_name, object_keys: object_keys, region: aws_region)
  { statusCode: 200, body: JSON.generate(response) }
end

def process_analysis(osa_id:, analysis_json:, bucket_name:, object_keys:, region:)
  qaqc_col = []
  build_types = []
  object_keys.each do |object_key|
    #If you find a zip file try downloading it and adding the information to the error_col array of hashes.
    folder_name = analysis_json[:analysis_name] + "_" + osa_id
    fetch_status, qaqc_col, build_types = process_file(file_id: object_key, analysis_json: analysis_json, bucket_name: bucket_name, qaqc_col: qaqc_col, region: region, build_types: build_types)
    if fetch_status == false
      return qaqc_col
    end
  end
  qaqc_col << build_types
  qaqc_col_file = analysis_json[:analysis_name] + "_" + osa_id.to_s + "/" + "issue_files.json"
  qaqc_status = put_data_s3(file_id: qaqc_col_file, bucket_name: bucket_name, data: qaqc_col, region: region)
  return true
end

def process_file(file_id:, analysis_json:, bucket_name:, qaqc_col:, region:, build_types:)
  if file_id.nil?
    return "No file name passed."
  else
    s3file = get_file_s3(file_id: file_id, bucket_name: bucket_name, region: region)
  end

  if s3file[:exist]
    qaqc_file = "out.osw"
    qaqc_json = unzip_files(zip_name: s3file[:file], search_name: qaqc_file)
    qaqc_json.each do |ind_json|
      sub_data = read_osw(osw_info: JSON.parse(ind_json), object_key: file_id)
      unless sub_data.empty?
        qaqc_col << sub_data
        is_pres = false
        build_types.each do |build_type|
          if sub_data[:build_type] == build_type[:build_type]
            build_type[:number] += 1
            is_pres = true
          end
        end
        unless is_pres
          build_types << {
              build_type: sub_data[:build_type],
              number: 1
          }
        end
      end
    end
    fetch_status = true
  else
    fetch_status  = false
    message = "Could not find #{file_id}"
    return fetch_status, message
  end

  # Get rid of the datapoint file that was just downloaded.
  File.delete(s3file[:file])
  return fetch_status, qaqc_col, build_types
end

def get_file_s3(file_id:, bucket_name:, region:)
  s3 = Aws::S3::Resource.new(region: region)
  bucket = s3.bucket(bucket_name)
  ret_bucket = bucket.object(file_id)
  if ret_bucket.exists?
    #If you find an osw.zip file try downloading it and adding the information to the error_col array of hashes.
    download_loc = '/tmp/out.zip'
    zip_index = 0
    while zip_index < 10
      zip_index += 1
      ret_bucket.download_file(download_loc)
      zip_index = 11 if File.exist?(download_loc)
    end
    if zip_index == 10
      return {exist: false, file: nil}
    else
      return {exist: true, file: download_loc}
    end
  else
    return {exist: false, file: nil}
  end
end

# Source copied and modified from https://github.com/rubyzip/rubyzip.
# This extracts the data from a zip file that presumably contains a json file.  It returns the contents of that file in
# an array of hashes (if there were multiple files in the zip file.)
def unzip_files(zip_name:, search_name: nil)
  out_info = []
  Zip::File.open(zip_name) do |zip_file|
    zip_file.each do |entry|
      if search_name.nil?
        content = entry.get_input_stream.read
        out_info << content
      else
        if entry.name == search_name
          content = entry.get_input_stream.read
          out_info << content
        end
      end
    end
  end
  return out_info
end

def get_s3_stream(file_id:, bucket_name:, region:)
  s3_res = Aws::S3::Resource.new(region: region)
  bucket = s3_res.bucket(bucket_name)
  ret_bucket = bucket.object(file_id)
  if ret_bucket.exists?
    s3_cli = Aws::S3::Client.new(region: region)
    return_data = JSON.parse(s3_cli.get_object(bucket: bucket_name, key: file_id)[:body].string)
  else
    return_data = []
  end
  return return_data
end

def put_data_s3(file_id:, bucket_name:, data:, region:)
  out_data = JSON.generate(data)
  s3 = Aws::S3::Resource.new(region: region)
  bucket = s3.bucket(bucket_name)
  out_obj = bucket.object(file_id)
  out_obj.put(body: out_data)
end

def read_osw(osw_info:, object_key:)
  status = osw_info['completed_status']
  if (/FAIL/.match(status.upcase)).nil?
    return {}
  else
    return {
        status: status,
        object_key: object_key,
        build_type: osw_info['steps'][0]['arguments']['building_type'],
        weather_loc: osw_info['steps'][0]['arguments']['epw_file']
    }
  end
end