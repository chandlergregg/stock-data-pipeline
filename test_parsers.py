from parsers import parse_line

def test_parse_csv_Q():
    input = "2020-08-05,2020-08-05 09:30:00.0,Q,SYMA,2020-08-05 09:34:51.505,1,NYSE,75.30254839137037,100,75.35916738004924,100"
    correct_output = "[datetime.datetime(2020, 8, 5, 0, 0), 'Q', 'SYMA', 'NYSE', datetime.datetime(2020, 8, 5, 9, 34, 51, 505000),1, datetime.datetime(2020, 8, 5, 9, 30), None, Decimal('75.30254839137037'), 100, Decimal('75.35916738004924'), 100, 'Q']"
    assert parse_line(input) == correct_output

def test_parse_csv_T():
    input = "2020-08-05,2020-08-05 09:30:00.0,T,SYMA,2020-08-05 10:37:21.581,10,NYSE,79.19488165597565,912"
    correct_output = "[datetime.datetime(2020, 8, 5, 0, 0), 'T', 'SYMA', 'NYSE', datetime.datetime(2020, 8, 5, 10, 37, 21, 581000), \
        10, datetime.datetime(2020, 8, 5, 9, 30), Decimal('79.19488165597565'), None, None, None, None, 'T']"
    assert parse_line(input) == correct_output.strip('"')

def test_parse_json_Q():
    input = """{"trade_dt":"2020-08-05","file_tm":"2020-08-05 09:30:00.000","event_type":"Q","symbol":"SYMA","event_tm":"2020-08-05 09:36:55.284",\
        "event_seq_nb":1,"exchange":"NASDAQ","bid_pr":76.10016521142818,"bid_size":100,"ask_pr":77.9647975908747,"ask_size":100}"""
    correct_output = "[datetime.datetime(2020, 8, 5, 0, 0), 'Q', 'SYMA', 'NASDAQ', datetime.datetime(2020, 8, 5, 9, 36, 55, 284000), 1, datetime.datetime(2020, 8, 5, 9, 30), \
        None, Decimal('76.1001652114281768035652930848300457000732421875'), 100, Decimal('77.9647975908746957429684698581695556640625'), 100, 'Q']"
    assert parse_line(input) == correct_output

def test_parse_json_T():
    input = """{"trade_dt":"2020-08-05","file_tm":"2020-08-05 09:30:00.000","event_type":"T","symbol":"SYMA","execution_id":"EX-10","event_tm":"2020-08-05 10:38:50.046",\
        "event_seq_nb":10,"exchange":"NASDAQ","price":77.77570455205036,"size":509}"""
    correct_output = "[datetime.datetime(2020, 8, 5, 0, 0), 'T', 'SYMA', 'NASDAQ', datetime.datetime(2020, 8, 5, 10, 38, 50, 46000), 10, datetime.datetime(2020, 8, 5, 9, 30), Decimal('77.7757045520503567104242392815649509429931640625'), None, None, None, None, 'T']"
    assert parse_line(input) == correct_output