#!/usr/bin/env ruby

require 'nokogiri'
require 'elasticsearch'
require 'pp'

collid = ARGV[0]
collmgr_filename = ARGV[1]
query = ARGV[2]

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

browsefields = collid_el.xpath(%{string(field[@name="browsefields"])}).downcase.split('|').map { |v| v.to_sym }
sortflds = collid_el.xpath(%{string(field[@name="sortflds"])}).downcase.split('|').map { |v| v.to_sym}.filter { |v| v != :none }
srchflds = collid_el.xpath(%{string(field[@name="dfltsrchflds"])}).downcase.split('|').map { |v| v.to_sym}.filter { |v| v != :ic_all }

client = Elasticsearch::Client.new log: false
aggregations_settings = {}
browsefields.each do |field|
  aggregations_settings[field] = {
    terms: { field: "#{field.to_s}.keyword", size: 10 }
  }
end

index_name = "dlxs_#{collid}"
response = client.search index: index_name,
                          body: {
                            query: { match: { text: query } },
                            fields: [ :istruct_isentryid, :m_id, :m_iid ],
                            _source: false,
                            aggregations: aggregations_settings
                          }

pp response["aggregations"]