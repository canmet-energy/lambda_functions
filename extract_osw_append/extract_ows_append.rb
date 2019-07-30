#!/usr/bin/env ruby

#Ensure that ruby can find the gems this script uses.
gem_loc = '/usr/local/lib/ruby/gems/2.2.0/gems/'
gem_dirs = Dir.entries(gem_loc).select {|entry| File.directory? File.join(gem_loc,entry) and !(entry =='.' || entry == '..') }
gem_dirs.sort.each do |gem_dir|
  lib_loc = ''
  lib_loc = gem_loc + gem_dir + '/lib'
  $LOAD_PATH.unshift(lib_loc) unless $LOAD_PATH.include?(lib_loc)
end

require 'bundler'
require 'securerandom'
require 'aws-sdk-s3'
require 'aws-sdk-lambda'
require 'json'

require 'rest-client'
require 'fileutils'
require 'zip'
require 'parallel'
require 'optparse'
require 'json'
require 'base64'
require 'colored'
require 'csv'

# Extract data from the osw file and write it on the disk. This method also calls process_simulation_json method
# which appends the qaqc data to the simulations.json. it only happens if the measure `btap_results` exist with
# `btap_results_json_zip` variable stored as part of the measure

# @param osw_json [:hash] osw file in json hash format
# @param output_folder [:string] parent folder where the data from osw will be extracted to
# @param uuid [:string] UUID of the datapoint
# @param simulations_json_folder [:string] root folder of the simulations.json file
# # @param aid [:string] analysis ID
def extract_data_from_osw(osw_json:, uuid:, aid:)
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
        analysis_json = JSON.parse(RestClient.get("http://web:80/analyses/#{aid}.json", headers={}))
        json['analysis_id']=analysis_json['analysis']['_id']
        json['analysis_name']=analysis_json['analysis']['display_name']
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

out_dir = ARGV[0].to_s

#Set up s3 bucket info
region = 'us-east-1'
s3 = Aws::S3::Resource.new(region: region)
bucket_name = 'btapresultsbucket'
bucket = s3.bucket(bucket_name)

#Get time information used for error logging
time_obj = Time.new
curr_time = time_obj.year.to_s + "-" + time_obj.month.to_s + "-" + time_obj.day.to_s + "_" + time_obj.hour.to_s + ":" + time_obj.min.to_s + ":" + time_obj.sec.to_s + ":" + time_obj.usec.to_s

#Find the osw file.
curr_dir = Dir.pwd
main_dir = curr_dir[0..-4]
res_dirs = Dir.entries(main_dir).select {|entry| File.directory? File.join(main_dir,entry) and !(entry =='.' || entry == '..') }
out_file_loc = main_dir + out_dir + "/"
out_file = out_file_loc + "out.osw"
osa_id = ""
osd_id = ""
file_id = ""

#Check if the osw exists
if File.file?(out_file)
  #Get the analysis id and datapoint id from the file
  File.open(out_file, "r") do |f|
    f.each_line do |line|
      if line.match(/   \"osa_id\" : \"/)
        osa_id = line[15..-4]
      elsif line.match(/   \"osd_id\" : \"/)
        osd_id = line[15..-4]
      end
    end
    #If either the analysis id or datapoint id are missing from the osw file put an error log on S3
    if osa_id == "" || osd_id == ""
      file_id = "log_" + curr_time
      log_file_loc = "./" + file_id + "txt"
      log_file = File.open(log_file_loc, 'w')
      log_file.puts "Either could not find osa_id or osd_id in out.osw file."
      log_file.close
      log_obj = bucket.object("log/" + file_id)
      log_obj.upload_file(log_file_loc)
    else
      #Transfer osw to S3
      osw_file_id = osa_id + "/" + osd_id + ".osw"
      out_obj = bucket.object(osw_file_id)
      while out_obj.exists? == false
        out_obj.upload_file(out_file)
      end
      osw_json = JSON.parse(File.read(out_file))

      #Get qaqc_info and catch any errors that are returned
      qaqc_file_loc = './out_json_file.json'
      error_file_loc = './out_error_file.json'
      qaqc_info, error_info = extract_data_from_osw(osw_json: osw_json, uuid: osd_id, aid: osa_id)
      #Create temporary files for s3 upload.  I tried using streaming to stream the data objects to s3 but that seems to
      #have problems so used this, roundabout, ugly, method instead.
      File.open(qaqc_file_loc,"w") {|each_file| each_file.write(JSON.pretty_generate(qaqc_info))}
      File.open(error_file_loc,"w") {|each_file| each_file.write(JSON.pretty_generate(error_info))}

      #Transfer qaqc json to S3
      qaqc_file_id = osa_id + "/" + "qaqc_" + osd_id + ".json"
      qaqc_out_obj = bucket.object(qaqc_file_id)
      while qaqc_out_obj.exists? == false
        qaqc_out_obj.upload_file(qaqc_file_loc)
      end
      File.delete(qaqc_file_loc) if File.exist?(qaqc_file_loc)

      #Transfer error_info csv to S3
      error_file_id = osa_id + "/" + "error_" + osd_id + ".json"
      error_out_obj = bucket.object(error_file_id)
      while error_out_obj.exists? == false
        error_out_obj.upload_file(error_file_loc)
      end
      File.delete(error_file_loc) if File.exist?(error_file_loc)
    end
  end
else
  #If the osw is ont there push a log with the error onto s3
  file_id = "log_" + curr_time
  log_file_loc = "./" + file_id + "txt"
  log_file = File.open(log_file_loc, 'w')
  log_file.puts "#{out_file} could not be found."
  log_file.close
  log_obj = bucket.object("log/" + file_id)
  log_obj.upload_file(log_file_loc)
end

require 'bundler'
require 'aws-sdk-s3'
require 'json'

analysis_id = ARGV[0].to_s

region = 'us-east-1'
s3 = Aws::S3::Resource.new(region: region)
bucket_name = 'btapresultsbucket'
bucket = s3.bucket(bucket_name)

res_path = "/mnt/openstudio/server/assets/"
res_file = "results." + analysis_id + ".zip"
res_file_path = res_path + res_file

time_obj = Time.new
curr_time = time_obj.year.to_s + "-" + time_obj.month.to_s + "-" + time_obj.day.to_s + "_" + time_obj.hour.to_s + ":" + time_obj.min.to_s + ":" + time_obj.sec.to_s + ":" + time_obj.usec.to_s

if File.file?(res_file_path)
  file_id = analysis_id + "/" + "results.zip"
  out_obj = bucket.object(file_id)
  resp = []
  while out_obj.exists? == false
    out_obj.upload_file(res_file_path)
  end
else
  file_id = "log_" + curr_time
  log_file_loc = "./" + file_id + ".txt"
  log_file = File.open(log_file_loc, 'w')
  log_file.puts "#{res_file_path} could not be found."
  log_file.close
  log_obj = bucket.object("log/" + file_id)
  log_obj.upload_file(log_file_loc)
end