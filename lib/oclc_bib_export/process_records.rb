require 'marc_cleanup'

# Returns a hash of different errors with true or false
def fixed_field_check(record)
  {
    multiple_no_008: multiple_no_008?(record),
    bad_005: bad_005?(record),
    bad_006: bad_006?(record),
    bad_007: bad_007?(record),
    bad_008: bad_008?(record),
    fixed_field_char_errors: fixed_field_char_errors?(record),
    leader_errors: leader_errors?(record)
  }
end

def variable_field_check(record)
  {
    auth_code_error: auth_code_error?(record),
    invalid_indicators: invalid_indicators?(record),
    invalid_subfield_code: invalid_subfield_code?(record),
    empty_subfields: empty_subfields?(record),
    multiple_no_245: multiple_no_245?(record),
    missing_040c: missing_040c?(record),
    pair_880_errors: pair_880_errors?(record),
    has_130_240: has_130_240?(record),
    multiple_1xx: multiple_1xx?(record)
  }
end

def global_field_check(record)
  {
    repeatable_field_errors: repeatable_field_errors?(record),
    invalid_tag: invalid_tag?(record),
    bad_utf8: bad_utf8?(record),
    invalid_xml_chars: invalid_xml_chars?(record)
  }
end

def clean_record(record)
  record = bad_utf8_fix(record)
  record = leaderfix(record)
  record = extra_space_fix(record)
  record = invalid_xml_fix(record)
  record = composed_chars_normalize(record)
  record = tab_newline_fix(record)
  record = empty_subfield_fix(record)
  record = fix_008(record)
end
