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
require 'parallel'
require 'optparse'
require 'json'
require 'base64'
require 'colored'
require 'csv'

def handler(event:, context:)
  osa_id = event["osa_id"]
  bucket_name = event["bucket_name"]
  object_keys = event["object_keys"]
  cycle_count = event["cycle_count"]
  analysis_json = {
      analysis_id: event["analysis_json"]["analysis_id"],
      analysis_name: event["analysis_json"]["analysis_name"]
  }
  response = process_analysis(osa_id: osa_id, analysis_json: analysis_json, bucket_name: bucket_name, object_keys: object_keys, cycle_count: cycle_count)
  { statusCode: 200, body: JSON.generate(response) }
end

def process_analysis(osa_id:, analysis_json:, bucket_name:, object_keys:, cycle_count:)
  error_col = []
  qaqc_col = []
  region = 'us-east-1'
  s3 = Aws::S3::Resource.new(region: region)
  bucket = s3.bucket(bucket_name)

  object_keys.each do |object_key|
    #If you find an osw.zip file try downloading it and adding the information to the error_col array of hashes.
    folder_name = analysis_json[:analysis_name] + "_" + osa_id
    string_start = folder_name.size + 1
    osd_id = object_key[string_start..-5]
    qaqc_col, error_col = process_file(osa_id: osa_id, osd_id: osd_id, file_id: object_key, analysis_json: analysis_json, bucket_name: bucket_name, qaqc_col: qaqc_col, error_col: error_col)
    if qaqc_col == false
      return error_col
    end
  end

  out_count = (cycle_count.to_i + 1).to_s

  qaqc_col_file = analysis_json[:analysis_name] + "_" + osa_id.to_s + "/" + "simulations_" + out_count + ".json"
  err_col_file = analysis_json[:analysis_name] + "_" + osa_id.to_s + "/" + "error_col_" + out_count + ".json"
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

# Source copied and modified from https://github.com/rubyzip/rubyzip.
# This extracts the data from a zip file that presumably contains a json file.  It returns the contents of that file in
# an array of hashes (if there were multiple files in the zip file.)
def unzip_osw(zip_file:)
  osw_json = []
  Zip::File.open(zip_file) do |file|
    file.each do |entry|
      puts "Extracting #{entry.name}"
      osw_json << JSON.parse(entry.get_input_stream.read)
    end
  end
  return osw_json
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

# @param osw_json [:hash] osw file in json hash format
# @param output_folder [:string] parent folder where the data from osw will be extracted to
# @param uuid [:string] UUID of the datapoint
# @param simulations_json_folder [:string] root folder of the simulations.json file
# # @param aid [:string] analysis ID
def extract_data_from_osw(osw_json:, uuid:, aid:, analysis_json:)
  results = osw_json
  out_json = []
  error_return = []
  output_folder = './'
  #itterate through all the steps of the osw file
  results['steps'].each do |measure|
    #puts "measure.name: #{measure['name']}"
    meausre_results_folder_map = {
        'openstudio_results':[
            {
                'measure_result_var_name': "eplustbl_htm",
                'filename': "#{output_folder}/eplus_table/#{uuid}-eplustbl.htm"
            },
            {
                'measure_result_var_name': "report_html",
                'filename': "#{output_folder}/os_report/#{uuid}-os-report.html"
            }
        ],
        'btap_view_model':[
            {
                'measure_result_var_name': "view_model_html_zip",
                'filename': "#{output_folder}/3d_model/#{uuid}_3d.html"
            }
        ],
        'btap_results':[
            {
                'measure_result_var_name': "model_osm_zip",
                'filename': "#{output_folder}/osm_files/#{uuid}.osm"
            },
            {
                'measure_result_var_name': "btap_results_hourly_data_8760",
                'filename': "#{output_folder}/8760_files/#{uuid}-8760_hourly_data.csv"
            },
            {
                'measure_result_var_name': "btap_results_hourly_custom_8760",
                'filename': "#{output_folder}/8760_files/#{uuid}-8760_hour_custom.csv"
            },
            {
                'measure_result_var_name': "btap_results_monthly_7_day_24_hour_averages",
                'filename': "#{output_folder}/8760_files/#{uuid}-mnth_24_hr_avg.csv"
            },
            {
                'measure_result_var_name': "btap_results_monthly_24_hour_weekend_weekday_averages",
                'filename': "#{output_folder}/8760_files/#{uuid}-mnth_weekend_weekday.csv"
            },
            {
                'measure_result_var_name': "btap_results_enduse_total_24_hour_weekend_weekday_averages",
                'filename': "#{output_folder}/8760_files/#{uuid}-endusetotal.csv"
            }
        ]
    }

    # if the measure is btapresults, then extract the osw file and qaqc json
    # While processing the qaqc json file, add it to the simulations.json file
    if measure["name"] == "btap_results" && measure.include?("result")
      measure["result"]["step_values"].each do |values|
        # extract the qaqc json blob data from the osw file and save it
        # in the output folder
        next unless values["name"] == 'btap_results_json_zip'
        btap_results_json_zip_64 = values['value']
        json_string =  Zlib::Inflate.inflate(Base64.strict_decode64( btap_results_json_zip_64 ))
        json = JSON.parse(json_string)
        # indicate if the current model is a baseline run or not
        # json['is_baseline'] = "#{flags[:baseline]}"

        #add ECM data to the json file
        measure_data = []
        results['steps'].each_with_index do |measure, index|
          step = {}
          measure_data << step
          step['name'] = measure['name']
          step['arguments'] = measure['arguments']
          if measure.has_key?('result')
            step['display_name'] = measure['result']['measure_display_name']
            step['measure_class_name'] = measure['result']['measure_class_name']
          end
          step['index'] = index
          # measure is an ecm if it starts with ecm_ (case ignored)
          step['is_ecm'] = !(measure['name'] =~ /^ecm_/i).nil? # returns true if measure name starts with 'ecm_' (case ignored)
        end

        json['measures'] = measure_data

        # add analysis_id and analysis name to the json file
        json['analysis_id']=analysis_json[:analysis_id]
        json['analysis_name']=analysis_json[:analysis_name]
        ret_json, curr_error_return = process_simulation_json(json: json, uuid: uuid, aid: aid, osw_file: results)
        out_json << ret_json
        error_return << curr_error_return
        puts "#{uuid}.json ok"
      end
    end # if measure["name"] == "btapresults" && measure.include?("result")
  end # of grab step files
  return out_json, error_return
end

# This method will append qaqc data to simulations.json
#
# @param json [:hash] contains original qaqc json file of a datapoint
# @param simulations_json_folder [:string] root folder of the simulations.json file
# @param osw_file [:hash] contains the datapoint's osw file
def process_simulation_json(json:, uuid:, aid:, osw_file:)
  #modify the qaqc json file to remove eplusout.err information,
  # and add separate building information and uuid key
  #json contains original qaqc json file on start

  error_return = ""
  building_type = ""
  epw_file = ""
  template = ""

  # get building_type, epw_file, and template from btap_create_necb_prototype_building inputs
  # if possible
  osw_file['steps'].each do |measure|
    next unless measure["name"] == "btap_create_necb_prototype_building"
    building_type = measure['arguments']["building_type"]
    epw_file =      measure['arguments']["epw_file"]
    template =      measure['arguments']["template"]
  end

  if json.has_key?('eplusout_err')
    json_eplus_warn = json['eplusout_err']['warnings'] unless json['eplusout_err']['warnings'].nil?
    json_eplus_fatal = json['eplusout_err']['fatal'].join("\n") unless json['eplusout_err']['fatal'].nil?
    json_eplus_severe = json['eplusout_err']['severe'].join("\n") unless json['eplusout_err']['severe'].nil?

    json['eplusout_err']['warnings'] = json['eplusout_err']['warnings'].size
    json['eplusout_err']['severe'] = json['eplusout_err']['severe'].size
    json['eplusout_err']['fatal'] = json['eplusout_err']['fatal'].size
  else
    error_return = error_return + "ERROR: Unable to find eplusout_err #{uuid}.json\n"
  end
  json['run_uuid'] = uuid
  #puts "json['run_uuid'] #{json['run_uuid']}"
  bldg = json['building']['name'].split('-')
  json['building_type'] = (building_type == "" ? (bldg[1]) : (building_type)  )
  json['template'] = (template == "" ? (bldg[0]) : (template)  )

  # output the errors to the error_log
  begin
    # write building_type, template, epw_file, QAQC errors, and sanity check
    # fails to the comma delimited file
    bldg_type = json['building_type']
    city = (epw_file == "" ? (json['geography']['city']) : (epw_file)  )
    json_error = ''
    json_error = json['errors'].join("\n") unless json['errors'].nil?
    json_sanity = ''
    json_sanity = json['sanity_check']['fail'].join("\n") unless json['sanity_check'].nil?

    # Ignore some of the warnings that matches the regex. This feature is implemented
    # to reduce the clutter in the error log. Additionally, if the number of
    # lines exceed a limit, excel puts the cell contents in the next row
    regex_patern_match = ['Blank Schedule Type Limits Name input -- will not be validated',
                          'You may need to shorten the names']
    matches = Regexp.new(Regexp.union(regex_patern_match),Regexp::IGNORECASE)
    json_eplus_warn = json_eplus_warn.delete_if {|line|
      !!(line =~ matches)
    }
    json_eplus_warn = json_eplus_warn.join("\n") unless json_eplus_warn.nil?
    error_return = {
        bldg_type: bldg_type,
        template: template,
        city: city,
        json_error: json_error,
        json_sanity: json_sanity,
        json_eplus_warn: json_eplus_warn,
        json_eplus_fatal: json_eplus_fatal,
        json_eplus_sever: json_eplus_severe,
        analysis_id: json['analysis_id'],
        analysis_name: json['analysis_name'],
        run_uuid: uuid
    }
  rescue => exception
    puts "[Ignore] There was an error writing to the BTAP Error Log"
    puts exception
  end
  return json, error_return
end