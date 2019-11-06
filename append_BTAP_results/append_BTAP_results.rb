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