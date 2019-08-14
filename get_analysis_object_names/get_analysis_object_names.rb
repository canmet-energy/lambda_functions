require 'json'
require 'aws-sdk-s3'

def lambda_handler(event:, context:)
  osa_id = event["osa_id"]
  bucket_name = event["bucket_name"]
  object_names = search_objects(osa_id: osa_id, bucket_name: bucket_name)
  { statusCode: 200, body: JSON.generate(object_names) }
end

def search_objects(osa_id:, bucket_name:)
  region = 'us-east-1'
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
        object_names << bucket_info
      end
    end
  end
  return object_names
end