require 'oci8'
require 'date'
require 'marc_cleanup'
require 'net/ftp'
module OclcBibExport

  def ok_to_export_record_dump_query
    %(
      SELECT record_segment
      FROM bib_data
        JOIN bib_master
          ON bib_data.bib_id = bib_master.bib_id
      WHERE
        bib_master.export_ok = 'Y'
        AND bib_master.suppress_in_opac = 'N'
        AND (
          (bib_master.export_ok_date > TO_TIMESTAMP_TZ(:begin_date, 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM'))
          AND (bib_master.export_ok_date <= TO_TIMESTAMP_TZ(:end_date, 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM'))
          )
      ORDER BY bib_data.bib_id, seqnum
    )
  end

  # @param file_handle [String] Filename for the dumped records
  # @param dates [Hash] Optional begin and end dates;
  # time format is YYYY-MM-DD HH24:MI:SS.FF TZHTZM, e.g.
  # 2018-10-04 11:12:00.000000 -0400
  # default dates are 7 days ago (at midnight) to today(at midnight)
  def ok_to_export_record_dump(file_handle, dates = {}, conn = nil)
    time = Date.today
    begin_date = dates[:begin_date].nil? ? (time - 7).strftime('%Y-%m-%d %H:%M:%S.%6N %z') : dates[:begin_date]
    end_date = dates[:end_date].nil? ? time.strftime('%Y-%m-%d %H:%M:%S.%6N %z') : dates[:end_date]
    output = File.open(file_handle, 'w')
    conn = OCI8.new(ENV['VOYAGER_DB_USER'], ENV['VOYAGER_DB_PASSWORD'], ENV['VOYAGER_DB_NAME']) if conn.nil?
    query = ok_to_export_record_dump_query
    cursor = conn.parse(query)
    cursor.bind_param(':begin_date', begin_date)
    cursor.bind_param(':end_date', end_date)
    cursor.exec
    while row = cursor.fetch
      output.write(row.first)
    end
    output.close
    cursor.close
  end

  def pcc_record?(record)
    return false unless record['042']
    record['042']['a'] == 'pcc'
  end

  def send_oclc_file(file)
    Net::FTP.open(ENV['OCLC_SERVER']) do |ftp|
      ftp.login(ENV['OCLC_USER'], ENV['OCLC_PASSWORD'])
      ftp.chdir(ENV['OCLC_PATH'])
      ftp.putbinaryfile(file)
    end
  end
  def clean_record(record)
    record = MarcCleanup.bad_utf8_fix(record)
    record = MarcCleanup.leaderfix(record)
    record = MarcCleanup.extra_space_fix(record)
    record = MarcCleanup.invalid_xml_fix(record)
    record = MarcCleanup.composed_chars_normalize(record)
    record = MarcCleanup.tab_newline_fix(record)
    record = MarcCleanup.empty_subfield_fix(record)
  end
end
