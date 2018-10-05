require_relative './../lib/oclc_bib_export'
require 'mail'
conn = OCI8.new(ENV['VOYAGER_DB_USER'], ENV['VOYAGER_DB_PASSWORD'], ENV['VOYAGER_DB_NAME'])
dates = {}
end_date = Date.today
begin_date = end_date - 7
dates[:end_date] = end_date.strftime('%Y-%m-%d %H:%M:%S.%6N %z')
dates[:begin_date] = begin_date.strftime('%Y-%m-%d %H:%M:%S.%6N %z')
export_file = "./../extracts/extract_#{begin_date.strftime('%Y.%m.%d')}-#{end_date.strftime('%Y.%m.%d')}.mrc"
ok_to_export_record_dump(export_file, dates, conn)
pcc_prefix = ENV['PCC_FILE_PREFIX']
non_pcc_prefix = ENV['NON_PCC_FILE_PREFIX']
pcc_file = "./../extracts/#{pcc_prefix}.#{end_date.strftime('%Y.%m.%d_$H.%M')}.mrc"
non_pcc_file = "./../extracts/#{non_pcc_prefix}.#{end_date.strftime('%Y.%m.%d_$H.%M')}.mrc"
pcc_writer = MARC::Writer.new(pcc_file)
non_pcc_writer = MARC::Writer.new(non_pcc_file)
pcc_bibs = []
non_pcc_bibs = []
reader = MARC::Reader.new(export_file)
reader.each do |record|
  bib_id = record['001'].value.to_i
  record = clean_record(record)
  if pcc_record?(record)
    pcc_writer.write(record)
    pcc_bibs << bib_id
  else
    non_pcc_writer.write(record)
    non_pcc_bibs << bib_id
  end
end
pcc_writer.close
non_pcc_writer.close
pcc_report = "./../extracts/pcc_#{begin_date.strftime('%Y.%m.%d')}-#{end_date.strftime('%Y.%m.%d')}.log"
non_pcc_report = "./../extracts/nonpcc_#{begin_date.strftime('%Y.%m.%d')}-#{end_date.strftime('%Y.%m.%d')}.log"
if File.size?(pcc_file)
  send_oclc_file(pcc_file)
  File.open(pcc_report, 'w') do |output|
    pcc_bibs.sort!
    pcc_bibs.each do |id|
      output.puts(id)
    end
  end
end
if File.size?(non_pcc_file)
  send_oclc_file(non_pcc_file)
  File.open(non_pcc_report, 'w') do |output|
    non_pcc_bibs.sort!
    non_pcc_bibs.each do |id|
      output.puts(id)
    end
  end
end
