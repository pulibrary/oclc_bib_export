require_relative './../lib/oclc_bib_export'
require 'mail'

conn = OCI8.new(ENV['VOYAGER_DB_USER'], ENV['VOYAGER_DB_PASSWORD'], ENV['VOYAGER_DB_NAME'])
dates = {}
end_date = Date.today
begin_date = end_date - 7
dates[:end_date] = end_date.strftime('%Y-%m-%d %H:%M:%S.%6N %z')
dates[:begin_date] = begin_date.strftime('%Y-%m-%d %H:%M:%S.%6N %z')

Mail.defaults do
  delivery_method :sendmail
end
from_email = ENV['SYS_EMAIL']
to_email = ENV['OCLC_EXPORT_EMAILS'].split(',')
subject = "OCLC Data Sync Export #{begin_date.strftime('%Y-%m-%d')} to #{end_date.strftime('%Y-%m-%d')}"
message_body = "No records to export for the date range of #{begin_date.strftime('%Y-%m-%d')} to #{end_date.strftime('%Y-%m-%d')}."

pcc_prefix = ENV['PCC_FILE_PREFIX']
non_pcc_prefix = ENV['NON_PCC_FILE_PREFIX']
pcc_file = "./../extracts/#{pcc_prefix}.#{end_date.strftime('%Y.%m.%d_%H.%M')}.mrc"
non_pcc_file = "./../extracts/#{non_pcc_prefix}.#{end_date.strftime('%Y.%m.%d_%H.%M')}.mrc"
export_file = "./../extracts/extract_#{begin_date.strftime('%Y.%m.%d')}-#{end_date.strftime('%Y.%m.%d')}.mrc"
report_file = "./../extracts/extract_#{begin_date.strftime('%Y.%m.%d')}-#{end_date.strftime('%Y.%m.%d')}.log"

ok_to_export_record_dump(export_file, dates, conn)
pcc_bibs = []
non_pcc_bibs = []
if File.size?(export_file)
  output = File.open(report_file, 'w')
  output.write("bib_id\t")
  output.write("pcc\t")
  output.write("sparse\t")
  output.write("008_count\t")
  output.write("bad_005\t")
  output.write("bad_006\t")
  output.write("bad_007\t")
  output.write("bad_008\t")
  output.write("fixed_field_chars\t")
  output.write("leader_errors\t")
  output.write("auth_code_error\t")
  output.write("invalid_indicators\t")
  output.write("invalid_subfield_code\t")
  output.write("empty_subfields\t")
  output.write("245_count\t")
  output.write("missing_040c\t")
  output.write("pair_880_errors\t")
  output.write("130_240\t")
  output.write("multiple_1xx\t")
  output.write("repeatable_field_errors\t")
  output.write("invalid_tag\t")
  output.write("bad_utf8\t")
  output.puts('invalid_xml_chars')
  message_body = "See attached log for records exported for OCLC for the date range of #{begin_date.strftime('%Y-%m-%d')} to #{end_date.strftime('%Y-%m-%d')}."
  pcc_writer = MARC::Writer.new(pcc_file)
  non_pcc_writer = MARC::Writer.new(non_pcc_file)
  reader = MARC::Reader.new(export_file)
  reader.each do |record|
    bib_id = record['001'].value.to_i
    fixed_errors = fixed_field_check(record)
    variable_errors = variable_field_check(record)
    global_errors = global_field_check(record)
    sparse = sparse_record?(record)
    multiple_no_008 = fixed_errors[:multiple_no_008]
    bad_005 = fixed_errors[:bad_005]
    bad_006 = fixed_errors[:bad_006]
    bad_007 = fixed_errors[:bad_007]
    bad_008 = fixed_errors[:bad_008]
    fixed_field_char_errors = fixed_errors[:fixed_field_char_errors]
    leader_errors = fixed_errors[:leader_errors]
    auth_code_error = variable_errors[:auth_code_error]
    invalid_indicators = variable_errors[:invalid_indicators]
    invalid_subfield_code = variable_errors[:invalid_subfield_code]
    empty_subfields = variable_errors[:empty_subfields]
    multiple_no_245 = variable_errors[:multiple_no_245]
    missing_040c = variable_errors[:missing_040c]
    pair_880_errors = variable_errors[:pair_880_errors]
    has_130_240 = variable_errors[:has_130_240]
    multiple_1xx = variable_errors[:multiple_1xx]
    repeatable_field_errors = global_errors[:repeatable_field_errors]
    invalid_tag = global_errors[:invalid_tag]
    bad_utf8 = global_errors[:bad_utf8]
    invalid_xml_chars = global_errors[:invalid_xml_chars]
    record = clean_record(record)
    if pcc_record?(record)
      pcc_writer.write(record)
      pcc_bibs << bib_id
      pcc = true
    else
      non_pcc_writer.write(record)
      non_pcc_bibs << bib_id
      pcc = false
    end
    output.write("#{bib_id}\t")
    output.write("#{pcc}\t")
    output.write("#{sparse}\t")
    output.write("#{multiple_no_008}\t")
    output.write("#{bad_005}\t")
    output.write("#{bad_006}\t")
    output.write("#{bad_007}\t")
    output.write("#{bad_008}\t")
    output.write("#{fixed_field_char_errors}\t")
    output.write("#{leader_errors}\t")
    output.write("#{auth_code_error}\t")
    output.write("#{invalid_indicators}\t")
    output.write("#{invalid_subfield_code}\t")
    output.write("#{empty_subfields}\t")
    output.write("#{multiple_no_245}\t")
    output.write("#{missing_040c}\t")
    output.write("#{pair_880_errors}\t")
    output.write("#{has_130_240}\t")
    output.write("#{multiple_1xx}\t")
    output.write("#{repeatable_field_errors}\t")
    output.write("#{invalid_tag}\t")
    output.write("#{bad_utf8}\t")
    output.puts(invalid_xml_chars)
  end
  pcc_writer.close
  non_pcc_writer.close
  File.unlink(pcc_file) unless File.size?(pcc_file)
  File.unlink(non_pcc_file) unless File.size?(non_pcc_file)
  output.close
  if File.size?(pcc_file)
    send_oclc_file(pcc_file)
  end
  if File.size?(non_pcc_file)
    send_oclc_file(non_pcc_file)
  end
else
  File.unlink(export_file)
end
Mail.deliver do
  from        "#{from_email}"
  to	        "#{to_email.join(', ')}"
  subject     "#{subject}"
  body        "#{message_body}"
  add_file report_file if File.exist?(report_file)
end
