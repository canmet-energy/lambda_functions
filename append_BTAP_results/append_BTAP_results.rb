require 'rubygems'
require 'aws-sdk-s3'
require 'json'

def handler(event:, context:)
  osa_id = event["osa_id"]
  bucket_name = event["bucket_name"]
  cycle_count = event["cycle_count"]
  append_tag = event["append_tag"]
  response = process_results(osa_id: osa_id, bucket_name: bucket_name, append_tag: append_tag, cycle_count: cycle_count)
  { statusCode: 200, body: JSON.generate(response) }
end

def process_results(osa_id:, bucket_name:, append_tag:, cycle_count:)
  region = 'us-east-1'
  s3 = Aws::S3::Resource.new(region: region)
  s3_client = AW::S3::Client.new(region: region)
  bucket = s3.bucket(bucket_name)
  for result_num in 1..cycle_count
    puts 'hello'
    res_tag = osa_id + '/' + append_tag + '_' + result_num.to_s + '.json'
    res_obj = bucket.object(res_tag)
    if res_obj.exists?
      res_stream = s3_client.body.read
    end
  end

  object_keys.each do |object_key|
    #If you find an osw.zip file try downloading it and adding the information to the error_col array of hashes.
    string_start = osa_id.size + 1
    osd_id = object_key[string_start..-5]
    qaqc_col, error_col = process_file(osa_id: osa_id, osd_id: osd_id, file_id: object_key, analysis_json: analysis_json, bucket_name: bucket_name, qaqc_col: qaqc_col, error_col: error_col)
    if qaqc_col == false
      return error_col
    end
  end

  out_count = (cycle_count.to_i + 1).to_s

  qaqc_col_file = osa_id.to_s + "/" + "simulations_" + out_count + ".json"
  err_col_file = osa_id.to_s + "/" + "error_col_" + out_count + ".json"
  qaqc_col_data = get_s3_stream(file_id: qaqc_col_file, bucket_name: bucket_name)
  qaqc_col_data << qaqc_col
  err_col_data = get_s3_stream(file_id: err_col_file, bucket_name: bucket_name)
  err_col_data << error_col
  qaqc_status = put_data_s3(file_id: qaqc_col_file, bucket_name: bucket_name,data: qaqc_col_data)
  err_status = put_data_s3(file_id: err_col_file, bucket_name: bucket_name,data: err_col_data)
  return true
end

def process_file(osa_id:, osd_id:, file_id:, analysis_json:, bucket_name:, qaqc_col:, error_col:)
  if file_id.nil?
    return "No file name passed."
  else
    s3file = get_file_s3(file_id: file_id, bucket_name: bucket_name)
  end
  if s3file[:exist]
    osw_json = unzip_osw(zip_file: s3file[:file])
  else
    file_exist = false
    message = "Could not find #{file_id}"
    return file_exist, message
  end
  osw_json.each do |osw|
    aid = osw['osa_id']
    uuid = osw['osd_id']
    if aid.nil? || uuid.nil?
      puts "Error either aid: #{aid} or uuid: #{uuid} not present"
    else
      qaqc, error_info = extract_data_from_osw(osw_json: osw, uuid: uuid, aid: aid, analysis_json: analysis_json)
      qaqc.each do |qaqc_ind|
        qaqc_col << qaqc_ind
      end
      error_info.each do |error_ind|
        error_col << error_ind
      end
    end
  end
  # Get rid of the datapoint osw file that was just downloaded.
  File.delete(s3file[:file])
  return qaqc_col, error_col
end

def get_file_s3(file_id:, bucket_name:)
  region = 'us-east-1'
  s3 = Aws::S3::Resource.new(region: region)
  bucket = s3.bucket(bucket_name)
  ret_bucket = bucket.object(file_id)
  if ret_bucket.exists?
    #If you find an osw.zip file try downloading it and adding the information to the error_col array of hashes.
    download_loc = '/tmp/out.zip'
    osw_index = 0
    while osw_index < 10
      osw_index += 1
      ret_bucket.download_file(download_loc)
      osw_index = 11 if File.exist?(download_loc)
    end
    if osw_index == 10
      return {exist: false, file: nil}
    else
      return {exist: true, file: download_loc}
    end
  else
    return {exist: false, file: nil}
  end
end

def get_s3_stream(file_id:, bucket_name:)
  region = 'us-east-1'
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

def put_data_s3(file_id:, bucket_name:, data:)
  out_data = JSON.pretty_generate(data)
  region = 'us-east-1'
  s3 = Aws::S3::Resource.new(region: region)
  bucket = s3.bucket(bucket_name)
  out_obj = bucket.object(file_id)
  out_obj.put(body: out_data)
end