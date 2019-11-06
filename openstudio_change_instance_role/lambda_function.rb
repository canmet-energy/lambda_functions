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
require 'aws-sdk-ec2'

def lambda_handler(event:, context:)
  ec2_instance_id = event["detail"]["instance-id"]
  ec2 = Aws::EC2::Client.new(region: 'us-east-1')
  instance_info = ec2.describe_instances({
                                             instance_ids: [ec2_instance_id]
                                         })
  ami_id = instance_info.reservations[0].instances[0].image_id
  iam_profile_info = instance_info.reservations[0].instances[0].iam_instance_profile
  test_arn = "arn:aws:iam::237788425317:instance-profile/ec2_s3_lambda"
  test_ami_id = "ami-095aa48abe1410bb2"
  #test_ami_id = "ami-0cfee17793b08a293"
  resp = ami_id
  if ami_id == test_ami_id
    if iam_profile_info.nil?
      resp = ec2.associate_iam_instance_profile({
                                                    iam_instance_profile: {
                                                        arn: test_arn
                                                    },
                                                    instance_id: ec2_instance_id
                                                })
    else
      resp = iam_profile_info
    end
  end
  response = {
      info: event,
      ami_id: ami_id,
      resp: resp
  }
  { statusCode: 200, body: JSON.generate(response) }
end