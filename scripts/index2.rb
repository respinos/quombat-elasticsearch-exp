#!/usr/bin/env ruby

require 'nokogiri'
require 'elasticsearch'
require 'pp'

collid = ARGV[0]
collmgr_filename = ARGV[1]
input_filename = ARGV[2]
browse_filename = ARGV[3]

collmgr_doc = File.open(collmgr_filename) { |f| Nokogiri::XML(f) }
collid_el = collmgr_doc.xpath(%{//row[field[@name="collid"] = "#{collid}"]})[0]

field_admin_maps = {}
line = collid_el.xpath(%{string(field[@name="field_admin_maps"])})
line.downcase.split('|').each do |line|
  key, values = line.split(':::')
  field_admin_maps[key.to_sym] = values.strip().split(' ').map { |f| f.to_sym }
end

field_xcoll_maps = {}
line = collid_el.xpath(%{string(field[@name="field_xcoll_maps"])})
line.downcase.split('|').each do |line|
  key, values = line.split(':::')
  if values[0] == '"'
    field_xcoll_maps[key.to_sym] = values[1..-2]
  else
    field_xcoll_maps[key.to_sym] = values.strip().split(' ').map { |f| f.to_sym }
  end
end


# build the DC mapping
dc_map = {}
[ 
  :dc_ti, 
  :dc_cr, 
  :dc_su, 
  :dc_de, 
  :dc_id, 
  :dc_pu, 
  :dc_da, 
  :dc_fo, 
  :dc_so, 
  :dc_rel, 
  :dc_type,
  :dc_ri,
  :dc_la,
  :dc_cov,
  :dc_ge
].each do |key|
  unless field_xcoll_maps[key].nil?
    dc_map[key] = field_xcoll_maps[key]
  end
end

browsefields = collid_el.xpath(%{string(field[@name="browsefields"])}).downcase.split('|').map { |v| v.to_sym }
sortflds = collid_el.xpath(%{string(field[@name="sortflds"])}).downcase.split('|').map { |v| v.to_sym}.filter { |v| v != :none }
srchflds = collid_el.xpath(%{string(field[@name="dfltsrchflds"])}).downcase.split('|').map { |v| v.to_sym}.filter { |v| v != :ic_all }

ic_all = field_admin_maps[:ic_all]

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

index_name = "dlxs_#{collid}"
client = Elasticsearch::Client.new log: true

if client.indices.exists?(index: index_name)
  client.indices.delete(index: index_name, ignore_unavailable: true)
end

# build the map of browse items
map = {}
xml_doc = File.open(browse_filename) { |f| Nokogiri::XML(f) }
xml_doc.xpath('//row').each do |row_el|
  idno = row_el.xpath('string(field[@name="idno"])')
  field = row_el.xpath('string(field[@name="field"])').to_sym
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
rows = xml_doc.xpath('//row')
xml_doc.xpath('//row').each do |row|
  body = {}
  idno = row.xpath('string(field[@name="istruct_isentryid"])')
  buffer = []
  ic_all.each do |field|
    value = row.xpath("string(field[@name='#{field}'])")
    next if value.nil? or value.empty?
    buffer << value.split('|||')
  end
  body[:text] = buffer.flatten.join(" ")
  body[:idno] = idno
  body[:m_id] = row.xpath('string(field[@name="m_id"])')
  body[:m_iid] = row.xpath('string(field[@name="m_iid"])')
  body[:istruct_ms] = row.xpath('string(field[@name="istruct_ms"])')

  dc_map.keys.each do |dc_field|
    values = []
    if dc_map[dc_field].is_a?(Array)
      dc_map[dc_field].each do |field|
        value = row.xpath("string(field[@name='#{field}'])").split('|||')
        next if value.nil? or value.empty?
        values << value
      end
    else
      values << dc_map[dc_field]
    end
    unless values.empty?
      values.flatten!
      body[dc_field] = values
    end
  end

  possible_maps = map[idno] || {}
  srchflds.each do |field|
    next unless possible_maps[field].nil?
    value = row.xpath("string(field[@name='#{field}'])")
    next if value.nil? or value.empty?
    body[field] = value.split('|||')
  end

  possible_maps.keys.each do |field|
    # we have individual details
    body[field] = []
    map[idno][field].each do |v|
      body[field] << v
    end
  end

  client.index(index: index_name, body: body)
  STDERR.puts "-- #{idno}"
  

end

STDERR.puts "=== #{collid}"