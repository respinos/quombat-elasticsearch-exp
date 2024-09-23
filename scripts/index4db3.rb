#!/usr/bin/env ruby

require 'nokogiri'
require 'elasticsearch'
require 'pp'
require 'sequel'

t0 = Time.now

collid = ARGV[0]
browse_filename = ARGV[1]

DSN = 'mysql2://127.0.0.1:3306/dlxs?user=root'
DB = Sequel.connect(DSN)

collmgr = DB[:Collection].join(:ImageClass, :collid => :collid, :userid => :userid).where(Sequel.lit('Collection.userid = ? AND Collection.collid = ?', 'roger', collid)).order(Sequel.lit('Collection.collid')).to_a.first

field_admin_maps = {}
line = collmgr[:field_admin_maps]
line.downcase.split('|').each do |line|
  key, values = line.split(':::')
  field_admin_maps[key.to_sym] = values.strip().split(' ').map { |f| f.to_sym }
end

field_xcoll_maps = {}
line = collmgr[:field_xcoll_maps]
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

browsefields = collmgr[:browsefields]&.downcase&.split('|')&.map { |v| v.to_sym } || []
sortflds = collmgr[:sortflds]&.downcase&.split('|')&.map { |v| v.to_sym}&.filter { |v| v != :none } || []
srchflds = collmgr[:dfltsrchflds]&.downcase.split('|')&.map { |v| v.to_sym}&.filter { |v| v != :ic_all } || []

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

index_name = "dlxs_#{collid}"
client = Elasticsearch::Client.new log: false

if client.indices.exists?(index: index_name)
  client.indices.delete(index: index_name, ignore_unavailable: true)
end

n = 0
stoppable = ARGV[2]&.to_i || -1
limit = 10000
offset = 0

update = []
while true
  break if stoppable > 0 && n >= stoppable
  rows = DB["#{collid}".to_sym].join("#{collid}_media".to_sym, :m_id => :ic_id).where(m_searchable: 1).limit(limit, offset).to_a
  break if rows.length == 0
  rows.each do |row|
    body = {}
    idno = row[:istruct_isentryid]
    buffer = []
    ic_all.each do |field|
      value = row[field]
      next if value.nil? or value.empty?
      buffer << value.split('|||')
    end
    body[:text] = buffer.flatten.join(" ")
    body[:idno] = idno
    body[:m_id] = row[:m_id]
    body[:m_iid] = row[:m_iid]
    body[:istruct_ms] = row[:istruct_ms]

    dc_map.keys.each do |dc_field|
      values = []
      if dc_map[dc_field].is_a?(Array)
        dc_map[dc_field].each do |field|
          value = row[field]&.split('|||')
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

    # build the map of browse items
    possible_maps = map[idno] || {}
    srchflds.each do |field|
      next unless possible_maps[field].nil?
      value = row[field]
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

    # client.index(index: index_name, body: body)
    update << { index: { _index: index_name, data: body } }
    n += 1
    STDERR.puts "-- #{n} / #{offset} :: #{idno}"
    

  end

  client.bulk(body: update)
  offset += limit
  update = []
end

STDERR.puts "=== #{collid} :: #{n} :: #{Time.now - t0}"