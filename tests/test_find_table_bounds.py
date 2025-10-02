from transform_merge import find_table_bounds

def _lines(text: str):
    # Split preserving newline behavior; function expects a sequence of lines
    return text.splitlines(True)

def test_find_table_bounds_simple():
    text = (
        "Some metadata\n"
        "More metadata\n"
        "COUNTRY,SEX,YEAR,VALUE\n"
        "ARM,MALE,2009,34.3\n"
        "ARM,FEMALE,2009,20.6\n"
        "Last update,2022.12.05\n"
    )
    header_idx, data_rows = find_table_bounds(_lines(text), ("COUNTRY","SEX","YEAR","VALUE"))
    assert header_idx == 2
    assert data_rows == 2  # two contiguous data lines after header

def test_find_table_bounds_no_footer_counts_to_end():
    text = (
        "Intro\n"
        "COUNTRY,SEX,YEAR,VALUE\n"
        "AUT,MALE,2005,29\n"
        "AUT,FEMALE,2005,23\n"
    )
    header_idx, data_rows = find_table_bounds(_lines(text), ("COUNTRY","SEX","YEAR","VALUE"))
    assert header_idx == 1
    assert data_rows == 2  # counts until file ends

def test_find_table_bounds_raises_if_no_header():
    text = "Metadata only\nNo columns here\n"
    try:
        find_table_bounds(_lines(text), ("COUNTRY","SEX","YEAR","VALUE"))
        assert False, "Expected ValueError"
    except ValueError:
        assert True
