#!/usr/bin/env ruby

require 'nokogiri'
require 'elasticsearch'
require 'pp'

input_filename = ARGV[0]
browse_filename = ARGV[1]

dc_map = {}
dc_map[:dc_ti] = [ :sdlhomes_street_no, :sdlhomes_historic_name, :sdlhomes_common_name ]
dc_map[:dc_de] = [ :sdlhomes_photo_view, :sdlhomes_desc ]
dc_map[:dc_id] = [ :sdlhomes_id ]
dc_map[:dc_pu] = [ "Saline District Library" ]
dc_map[:dc_da] = [ :dc_da ]

dlxs_ma = [ :sdlhomes_street_no, :sdlhomes_history_name ]

browsefields = [
  :sdlhomes_orig_usage,
  :sdlhomes_present_usage,
  :sdlhomes_ownership,
  :sdlhomes_date_construction,
  :sdlhomes_builder,
]

sortflds = [ :sdlhomes_photo_date, :sdlhomes_survey_dt, :sdlhomes_photo_neg_no ]
ic_all = [ 
  :sdlhomes_builder, 
  :sdlhomes_ref,
  :sdlhomes_block_no,
  :sdlhomes_card_no, 
  :sdlhomes_common_name, 
  :sdlhomes_context, 
  :sdlhomes_county, 
  :sdlhomes_date_construction, 
  :sdlhomes_desc, 
  :sdlhomes_district_name, 
  :sdlhomes_fn,
  :sdlhomes_form_source,
  :sdlhomes_historic_name,
  :sdlhomes_municipal_unit, 
  :sdlhomes_national_register_listed, 
  :sdlhomes_orig_usage, 
  :sdlhomes_ownership,
  :sdlhomes_photo_date, 
  :sdlhomes_photo_neg_no,
  :sdlhomes_photo_view, 
  :sdlhomes_present_usage, 
  :sdlhomes_id, 
  :sdlhomes_recorder_dt, 
  :sdlhomes_sub_unit, 
  :sdlhomes_survey_dt, 
  :sdlhomes_surveyor, 
  :sdlhomes_usgs_map, 
  :sdlhomes_36_cfr_61,
  :sdlhomes_street_no
]

srchflds = [
  :sdlhomes_historic_name,
  :sdlhomes_common_name,
  :sdlhomes_district_name,
  :sdlhomes_ref,
  :sdlhomes_photo_neg_no,
]

index_settings = {}
index_settings[:mappings] = {}
sortflds.each do |field|
  index_settings[:mappings][field] = {
    type: "text",
    fields: {
      raw: {
        type: "keyword"
      }
    }
  }
end

client = Elasticsearch::Client.new log: true
# client.indicies.create({
#   index: "sdlhomes_index",
#   body: {
#     settings: {
#       index: index_settings
#     }
#   }
# })

# build the map of browse items
map = {}
xml_doc = File.open(browse_filename) { |f| Nokogiri::XML(f) }
xml_doc.xpath('//row').each do |row_el|
  idno = row_el.xpath('string(field[@name="idno"])')
  field = row_el.xpath('string(field[@name="field"])')
  value = row_el.xpath('string(field[@name="value"])')
  if map[idno].nil?
    map[idno] = {}
  end
  if map[idno][field].nil?
    map[idno][field] = []
  end
  map[idno][field] << value
end

xml_doc = File.open(input_filename) { |f| Nokogiri::XML(f) }
xml_doc.xpath('//row').each do |row|
  body = {}
  idno = row.xpath('string(field[@name="istruct_isentryid"])')
  buffer = []
  ic_all.each do |field|
    value = row.xpath("string(field[@name='#{field}'])")
    next if value.nil? or value.empty?
    buffer << value
  end
  body[:text] = buffer.join(" ")
  body[:idno] = idno
  body[:m_id] = row.xpath('string(field[@name="m_id"])')
  body[:m_iid] = row.xpath('string(field[@name="m_iid"])')
  body[:istruct_ms] = row.xpath('string(field[@name="istruct_ms"])')

  dc_map.keys.each do |dc_field|
    values = []
    dc_map[dc_field].each do |field|
      value = row.xpath("string(field[@name='#{field}'])")
      next if value.nil? or value.empty?
      values << value
    end
    unless values.empty?
      body[dc_field] = values
    end
  end

  possible_maps = map[idno] || {}
  srchflds.each do |field|
    next unless possible_maps[field].nil?
    value = row.xpath("string(field[@name='#{field}'])")
    next if value.nil? or value.empty?
    body[field] = value
  end
  possible_maps.keys.each do |field|
    # we have individual details
    body[field] = []
    map[idno][field].each do |v|
      body[field] << v
    end
  end

  client.index(index: 'dlxs_sdlhomes', body: body)
  STDERR.puts "-- #{idno}"
  

end