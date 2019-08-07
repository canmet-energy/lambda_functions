require 'json'

osa_id = '9314d10d-b297-4b1f-9770-e8ad4a1a05f8'
#datapoint_ids = [
#    '1c56ca9e-b637-4969-be12-3936e1f4b3ca',
#    '2c49b2fe-d100-485e-a5a4-304712f316b4',
#    '489c5c47-01d8-4fea-9743-b0f2b00a42b6',
#    '5ecb75cb-8a08-4aa5-bcc0-81dd3f032469',
#    '620c973c-a883-44dc-babc-8bc1f69a70c1',
#    '64e0f088-85fd-400b-b949-72cc72445ab3',
#    'b762a70b-e174-4fd7-84a4-9fcd9071cac5',
#    'b8e136f7-6522-4a4e-a838-2b8a8d705156'
#]

datapoint_ids = [
    '1c56ca9e-b637-4969-be12-3936e1f4b3ca'
]
event = {}
datapoint_ids.each do |osd_id|
  file_id = osa_id + '/' + osd_id + '.zip'
  analysis_json = {
      analysis_id: osa_id,
      analysis_name: 'none'
  }
  event = {
      osa_id: osa_id,
      osd_id: osd_id,
      file_id: file_id,
      analysis_json: analysis_json
  }
end

json_out = "./test.json"
File.open(json_out,"w") {|each_file| each_file.write(JSON.pretty_generate(event))}