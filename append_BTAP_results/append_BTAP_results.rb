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
  s3_cli = Aws::S3::Client.new(region: aws_region)
  response = process_results(osa_id: osa_id, bucket_name: bucket_name, append_tag: append_tag, cycle_count: cycle_count, analysis_json: analysis_json, region: aws_region, s3_cli: s3_cli)
  { statusCode: 200, body: JSON.generate(response) }
end

def process_results(osa_id:, bucket_name:, append_tag:, cycle_count:, analysis_json:, region:, s3_cli:)
  res_comp = "["
  for result_num in 1..cycle_count
    res_key = analysis_json[:analysis_name] + '_' + osa_id + '/' + append_tag + '_' + result_num.to_s + '.json'
    res_comp << s3_cli.get_object(bucket: bucket_name, key: res_key).body.read[1..-2] + ','
  end
  res_comp[-1] = ']'
  out_key = analysis_json[:analysis_name] + '_' + osa_id + '/' + append_tag + '.json'
  resp = s3_cli.put_object({
                               body: res_comp,
                               bucket: bucket_name,
                               key: out_key
                           })
  return resp
end