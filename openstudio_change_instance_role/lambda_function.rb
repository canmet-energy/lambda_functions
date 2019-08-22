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