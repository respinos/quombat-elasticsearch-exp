#!/usr/bin/env ruby

require 'nokogiri'
require 'elasticsearch'
require 'pp'

browsefields = [:hart_ti,
 :hart_vt,
 :hart_cr,
 :hart_nln,
 :hart_lo,
 :hart_da,
 :hart_su,
 :hart_wono,
 :hart_ordno]

client = Elasticsearch::Client.new log: false
aggregations_settings = {}
browsefields.each do |field|
  aggregations_settings[field] = {
    terms: { field: "#{field.to_s}.keyword", size: 10 }
  }
end
response = client.search index: 'dlxs_hart',
                          body: {
                            query: { match: { text: 'painter'} },
                            fields: [ :istruct_isentryid, :m_id, :m_iid ],
                            _source: false,
                            aggregations: aggregations_settings
                          }

pp response["aggregations"]