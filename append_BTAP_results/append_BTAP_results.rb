require 'aws-sdk-s3'
require 'json'

def handler(event:, context:)
  osa_id = event["osa_id"]
  bucket_name = event["bucket_name"]
  cycle_count = event["cycle_count"]
  append_tag = event["append_tag"]
  analysis_json = {
      analysis_id: event["analysis_json"]["analysis_id"],
      analysis_name: event["analysis_json"]["analysis_name"]
  }
  aws_region = "us-east-1"
  unless event["region"].nil?
    aws_region = event["region"]
  end
  response = process_results(osa_id: osa_id, bucket_name: bucket_name, append_tag: append_tag, cycle_count: cycle_count, analysis_json: analysis_json, region: aws_region)
  { statusCode: 200, body: JSON.generate(response) }
end

def process_results(osa_id:, bucket_name:, append_tag:, cycle_count:, analysis_json:, region:)
  res_comp = []
  for result_num in 1..cycle_count
    res_key = analysis_json[:analysis_name] + '_' + osa_id + '/' + append_tag + '_' + result_num.to_s + '.json'
    res_json = get_s3_stream(file_id: res_key, bucket_name: bucket_name, region: region)
    if res_json.empty? || res_json.nil?
      return "Could not get object with key #{res_key} in bucket #{bucket_name} in region #{region}."
    else
      res_comp.concat(res_json)
    end
  end
  out_key = analysis_json[:analysis_name] + '_' + osa_id + '/' + append_tag + '.json'
  res_out = put_data_s3(file_id: out_key, bucket_name: bucket_name, data: res_comp, region: region)
  return res_out
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
  while out_obj.exists? == false
    res = out_obj.put(body: out_data)
  end
  return res
end