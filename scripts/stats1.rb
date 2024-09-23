#!/usr/bin/env ruby

require 'nokogiri'
require 'elasticsearch'
require 'pp'

collids = ['sclaudubon', 'sdlhomes', 'mlibrary1ic', 'clark1ic8lift', 'ccs1ic', 'dance1ic', 'herb00ic8lift', 'hart', 'apis', 'bhl']
if ARGV[0]
  collids = [ ARGV ].flatten
end

client = Elasticsearch::Client.new log: false
collids.each do |collid|
  index_name = "dlxs_#{collid}"
  response = client.indices.disk_usage(index: index_name, run_expensive_tasks: true)
  store_size = response[index_name]['store_size']
  response = client.indices.stats(index: index_name)
  num_docs = 0 
  num_docs = response["_all"]["total"]["docs"]["count"]
  puts "-- #{collid} :: #{store_size} :: #{num_docs}"
  
  # pp response
end