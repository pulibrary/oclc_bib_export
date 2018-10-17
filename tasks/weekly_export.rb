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
sparse = { description: 'are sparse', records: [] }
multiple_no_008 = { description: 'have multiple or no 008s', records: [] }
bad_005 = { description: 'have bad 005s', records: [] }
bad_006 = { description: 'have bad 006s', records: [] }
bad_007 = { description: 'have bad 007s', records: [] }
bad_008 = { description: 'have bad 008s', records: [] }
fixed_field_char_errors = { description: 'have fixed field character errors', records: [] }
leader_errors = { description: 'have leader errors', records: [] }
auth_code_error = { description: 'have 042 auth code errors', records: [] }
invalid_indicators = { description: 'have invalid indicators', records: [] }
invalid_subfield_code = { description: 'have invalid subfields', records: [] }
empty_subfields = { description: 'have empty subfields', records: [] }
multiple_no_245 = { description: 'have multiple or no 245s', records: [] }
missing_040c = { description: 'have no 040 $c', records: [] }
pair_880_errors = { description: 'have 880 pairing errors', records: [] }
has_130_240 = { description: 'have a 130 and 240', records: [] }
multiple_1xx = { description: 'have multiple 1xx fields', records: [] }
repeatable_field_errors = { description: 'have multiple non-repeatable fields', records: [] }
invalid_tag = { description: 'have invalid field tags', records: [] }
bad_utf8 = { description: 'have bad UTF-8 bytes', records: [] }
invalid_xml_chars = { description: 'have invalid XML 1.0 characters', records: [] }
if File.size?(export_file)
  message_body = "See attached log for records exported for OCLC for the date range of #{begin_date.strftime('%Y-%m-%d')} to #{end_date.strftime('%Y-%m-%d')}."
  pcc_writer = MARC::Writer.new(pcc_file)
  non_pcc_writer = MARC::Writer.new(non_pcc_file)
  reader = MARC::Reader.new(export_file)
  reader.each do |record|
    bib_id = record['001'].value.to_i
    fixed_errors = fixed_field_check(record)
    variable_errors = variable_field_check(record)
    global_errors = global_field_check(record)
    sparse[:records] << bib_id if sparse_record?(record)
    multiple_no_008[:records] << bib_id if fixed_errors[:multiple_no_008]
    bad_005[:records] << bib_id if fixed_errors[:bad_005]
    bad_006[:records] << bib_id if fixed_errors[:bad_006]
    bad_007[:records] << bib_id if fixed_errors[:bad_007]
    bad_008[:records] << bib_id if fixed_errors[:bad_008]
    fixed_field_char_errors[:records] << bib_id if fixed_errors[:fixed_field_char_errors]
    leader_errors[:records] << bib_id if fixed_errors[:leader_errors]
    auth_code_error[:records] << bib_id if fixed_errors[:auth_code_error]
    invalid_indicators[:records] << bib_id if fixed_errors[:invalid_indicators]
    invalid_subfield_code[:records] << bib_id if fixed_errors[:invalid_subfield_code]
    empty_subfields[:records] << bib_id if fixed_errors[:empty_subfields]
    multiple_no_245[:records] << bib_id if fixed_errors[:multiple_no_245]
    missing_040c[:records] << bib_id if fixed_errors[:missing_040c]
    pair_880_errors[:records] << bib_id if fixed_errors[:pair_880_errors]
    has_130_240[:records] << bib_id if fixed_errors[:has_130_240]
    multiple_1xx[:records] << bib_id if fixed_errors[:multiple_1xx]
    repeatable_field_errors[:records] << bib_id if fixed_errors[:repeatable_field_errors]
    invalid_tag[:records] << bib_id if fixed_errors[:invalid_tag]
    bad_utf8[:records] << bib_id if fixed_errors[:bad_utf8]
    invalid_xml_chars[:records] << bib_id if fixed_errors[:invalid_xml_chars]
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
  File.unlink(pcc_file) unless File.size?(pcc_file)
  File.unlink(non_pcc_file) unless File.size?(non_pcc_file)
  non_pcc_writer.close
  File.open(report_file, 'w') do |output|
    output.puts('PCC records exported')
    unless pcc_bibs.empty?
      pcc_bibs.each do |id|
        output.puts(id)
      end
    end
    output.puts("Total number of PCC records exported: #{pcc_bibs.size}")
    output.puts("\n")
    output.puts('non-PCC records exported')
    unless non_pcc_bibs.empty?
      non_pcc_bibs.each do |id|
        output.puts(id)
      end
    end
    output.puts("Total number of non-PCC records exported: #{non_pcc_bibs.size}")
    output.puts("\n\n")
    [
      sparse,
      multiple_no_008,
      bad_005,
      bad_006,
      bad_007,
      bad_008,
      fixed_field_char_errors,
      leader_errors,
      auth_code_error,
      invalid_indicators,
      invalid_subfield_code,
      empty_subfields,
      multiple_no_245,
      missing_040c,
      pair_880_errors,
      has_130_240,
      multiple_1xx,
      repeatable_field_errors,
      invalid_tag,
      bad_utf8,
      invalid_xml_chars,
    ].each do |hash|
      next if hash[:records].empty?
      output.puts("Records that #{hash[:description]}")
      hash[:records].each do |id|
        output.puts(id)
      end
      output.puts("Total number of errored records: #{hash[:records].size}")
      output.puts("\n")
    end
  end
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
