require 'json'

osa_id = '5d822d74-f567-4a36-b7c9-665768465b16'
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
    '5d822d74-f567-4a36-b7c9-665768465b16/2822704e-1b90-492d-a72b-2128fdbbf5c4.zip',
    '5d822d74-f567-4a36-b7c9-665768465b16/9318e5de-173a-464c-b8ae-b8bc9bcc88ec.zip',
    '5d822d74-f567-4a36-b7c9-665768465b16/94894105-8581-413b-aecf-4a55574f3912.zip',
    '5d822d74-f567-4a36-b7c9-665768465b16/97797585-0503-415f-978a-e5649d645e34.zip',
    '5d822d74-f567-4a36-b7c9-665768465b16/98b31989-000a-4817-9c53-0e94273b6d43.zip',
    '5d822d74-f567-4a36-b7c9-665768465b16/b01ebab7-b532-4b84-83a7-a64b79d132a9.zip',
    '5d822d74-f567-4a36-b7c9-665768465b16/d043ee8f-3465-4bf6-b61e-6f2a57a17898.zip',
    '5d822d74-f567-4a36-b7c9-665768465b16/e475b03d-fdab-4969-8b03-b4a23dff5d9c.zip'
]

#datapoint_ids = [
#    '1c56ca9e-b637-4969-be12-3936e1f4b3ca'
#]
bucket_name = "btapresultsbucket"
analysis_json = {
    analysis_id: osa_id,
    analysis_name: 'test_analysis'
}
event = {
    osa_id: osa_id,
    bucket_name: bucket_name,
    object_keys: datapoint_ids,
    cycle_count: 0,
    analysis_json: analysis_json
}

=begin
event = {}
datapoint_ids.each do |osd_id|
  file_id = osa_id + '/' + osd_id + '.zip'
  analysis_json = {
      analysis_id: osa_id,
      analysis_name: 'none'
  }
  event = {
      osa_id: osa_id,
      bucket_name: bucket_name,
      analysis_json: analysis_json
  }
end
=end
json_out = "./test.json"
File.open(json_out,"w") {|each_file| each_file.write(JSON.pretty_generate(event))}