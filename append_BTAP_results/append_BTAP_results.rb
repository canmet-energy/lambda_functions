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
  res_comp = []
  for result_num in 1..cycle_count
    res_key = osa_id + '/' + append_tag + '_' + result_num.to_s + '.json'
    res_json = get_s3_stream(file_id: res_key, bucket_name: bucket_name)
    if res_json.empty? || res_json.nil?
      return "Could not get object with key #{res_key} in bucket #{bucket_name}."
    else
      res_json.each do |ind_res|
        res_comp << ind_res
      end
    end
  end
  out_key = osa_id + '/' + append_tag + '.json'
  res_out = put_data_s3(file_id: out_key, bucket_name: bucket_name, data: res_comp)
  return res_out
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
  while out_obj.exists? == false
    out_obj.put(body: out_data)
  end
end