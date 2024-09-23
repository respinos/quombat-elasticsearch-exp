#!/usr/bin/env ruby

i = 0
do_skip_next = false
buffer = []
input_filename = ARGV[0]
File.open(input_filename, "rb").each do |line|
  if line.match?('"dlxs_sha"')
    do_skip_next = line.match?('</field>') != true
    # STDERR.puts line if do_skip_next
    next
  elsif do_skip_next
    STDERR.puts line
    do_skip_next = line.match?('</field>') != true
    next
  end

  buffer << line
end

File.rename(input_filename, "./xml/SHA/#{File.basename(input_filename)}")
File.open(input_filename, "wb") { |f| f.write(buffer.join("")) }
