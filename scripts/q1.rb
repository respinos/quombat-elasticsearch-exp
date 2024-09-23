#!/usr/bin/env ruby

require 'nokogiri'
require 'elasticsearch'
require 'pp'

browsefields = [
  :sdlhomes_orig_usage,
  :sdlhomes_present_usage,
  :sdlhomes_ownership,
  :sdlhomes_date_construction,
  :sdlhomes_builder,
]

client = Elasticsearch::Client.new log: false
aggregations_settings = {}
browsefields.each do |field|
  aggregations_settings[field] = {
    terms: { field: "#{field.to_s}.keyword", size: 10 }
  }
end
response = client.search index: 'dlxs_sdlhomes',
                          body: {
                            query: { match: { text: 'residence'} },
                            fields: [ :istruct_isentryid, :m_id, :m_iid ],
                            _source: false,
                            aggregations: aggregations_settings
                          }

pp response["aggregations"]