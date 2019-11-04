require 'rubygems'
require 'aws-sdk-s3'
require 'fileutils'
require 'zip'
require 'json'

def handler(event:, context:)
  osa_id = event["osa_id"]
  bucket_name = event["bucket_name"]
  object_keys = event["object_keys"]
  cycle_count = event["cycle_count"]
  aws_region = event["region"]
  analysis_json = {
      analysis_id: event["analysis_json"]["analysis_id"],
      analysis_name: event["analysis_json"]["analysis_name"]
  }
  response = process_analysis(osa_id: osa_id, analysis_json: analysis_json, bucket_name: bucket_name, object_keys: object_keys, cycle_count: cycle_count, region: aws_region)
  { statusCode: 200, body: JSON.generate(response) }
end

def process_analysis(osa_id:, analysis_json:, bucket_name:, object_keys:, cycle_count:, region:)
  qaqc_col = []
  missing_files = []
  upload_res = []

  object_keys.each do |object_key|
    #If you find a zip file try downloading it and adding the information to the error_col array of hashes.
    folder_name = analysis_json[:analysis_name] + "_" + osa_id
    return_info = process_file(file_id: object_key, analysis_json: analysis_json, bucket_name: bucket_name, qaqc_col: qaqc_col, region: region)
    if return_info[:fetch_status]
      qaqc_col << return_info[:message]
    else
      if object_key.nil?
        object_key = "unknown"
      end
      {
          object_key: object_key,
          message: return_info[:message]
      }
      missing_files << object_key
    end
  end

  out_count = (cycle_count.to_i + 1).to_s

  if qaqc_col.empty?
    qaqc_col << {
        message: "No results information could be found."
    }
  end
  qaqc_col_file = analysis_json[:analysis_name] + "_" + osa_id.to_s + "/" + "simulations_" + out_count + ".json"
  qaqc_col_data = get_s3_stream(file_id: qaqc_col_file, bucket_name: bucket_name, region: region)
  qaqc_col_data.concat(qaqc_col)
  upload_res << put_data_s3(file_id: qaqc_col_file, bucket_name: bucket_name,data: qaqc_col_data, region: region)
  unless missing_files.empty?
    missing_file_log = analysis_json[:analysis_name] + "_" + osa_id.to_s + "/" + "missing_files_" + out_count + ".json"
    upload_res << put_data_s3(file_id: missing_file_log, bucket_name: bucket_name,data: qaqc_col_data, region: region)
  end
  return upload_res
end

def process_file(file_id:, analysis_json:, bucket_name:, qaqc_col:, region:)
  output = {
      fetch_status: false,
      message: "No file name was passed."
  }
  if file_id.nil?
    return output
  else
    s3file = get_file_s3(file_id: file_id, bucket_name: bucket_name, region: region)
  end

  if s3file[:exist] == true
    qaqc_file = "qaqc.json"
    qaqc_json = unzip_files(zip_name: s3file[:file], search_name: qaqc_file)
    if qaqc_json[:status] == true
      qaqc_out = {}
      qaqc_json[:out_info].each do |ind_json|
        qaqc_out = JSON.parse(ind_json)
      end
      output[:fetch_status] = true
      output[:message] = qaqc_out
      # Get rid of the datapoint file that was just downloaded.
      File.delete(s3file[:file])
      return output
    else
      output[:fetch_status] = false
      output[:message] = "No qaqc.json file present in zip file."
      # Get rid of the datapoint file that was just downloaded.
      File.delete(s3file[:file])
      return output
    end
  else
    output[:fetch_status]  = false
    output[:message] = "Could not find #{file_id}"
    return output
  end
end

def get_file_s3(file_id:, bucket_name:, region:)
  s3 = Aws::S3::Resource.new(region: region)
  bucket = s3.bucket(bucket_name)
  ret_bucket = bucket.object(file_id)
  download_loc = "/tmp/out.zip"
  if File.exist?(download_loc)
    File.delete(download_loc)
  end
  if ret_bucket.exists?
    #If you find an osw.zip file try downloading it and adding the information to the error_col array of hashes.
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
  output = {
      status: false,
      out_info: []
  }
  Zip::File.open(zip_name) do |zip_file|
    zip_file.each do |entry|
      if search_name.nil?
        output[:status] = true
        content = entry.get_input_stream.read
        output[:out_info] << content
      else
        if entry.name == search_name
          output[:status] = true
          content = entry.get_input_stream.read
          output[:out_info] << content
        end
      end
    end
  end
  return output
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