import json
import datetime
from decimal import *

COMMON_EVENT_COLUMN_COUNT = 13

def parse_line(line: str):

    if line[0] == "{":
        output = parse_json(line)
    else:
        output = parse_csv(line)
    return output

def parse_csv(line: str) -> str:
    """
    Takes csv input line and turns into common_event record.
    Returns empty "bad" common record if anything goes wrong.

    Args: 
        line: csv line from file
    Returns:
        common_event string record
    """ 

    try:

        Q_column_headers = ['trade_dt','arrival_tm','rec_type','symbol','event_tm', \
            'event_seq_nb','exchange','bid_pr','bid_size','ask_pr','ask_size']
        T_column_headers = ['trade_dt','arrival_tm','rec_type','symbol','event_tm', \
            'event_seq_nb','exchange','trade_pr']
        
        bid_pr = None
        bid_size = None
        ask_pr = None
        ask_size = None
        trade_pr = None

        record = line.split(",")
        if record[2] == 'Q':
            record_dict = dict(zip(Q_column_headers, record))
            bid_pr = Decimal(record_dict['bid_pr'])
            bid_size = int(record_dict['bid_size'])
            ask_pr = Decimal(record_dict['ask_pr'])
            ask_size = int(record_dict['ask_size'])
        else:
            record_dict = dict(zip(T_column_headers, record))
            trade_pr = Decimal(record_dict['trade_pr'])

        trade_dt = datetime.datetime.strptime(record_dict['trade_dt'], '%Y-%m-%d')
        rec_type = record_dict['rec_type']
        symbol = record_dict['symbol']
        exchange = record_dict['exchange']
        event_tm = datetime.datetime.strptime(record_dict['event_tm'], '%Y-%m-%d %H:%M:%S.%f')
        event_seq_nb = int(record_dict['event_seq_nb'])
        arrival_tm = datetime.datetime.strptime(record_dict['arrival_tm'], '%Y-%m-%d %H:%M:%S.%f')
        partition = rec_type

        return [trade_dt, rec_type, symbol, exchange, event_tm, event_seq_nb, arrival_tm, \
            trade_pr, bid_pr, bid_size, ask_pr, ask_size, partition]
    
    except Exception as e:
    
        # If anything goes wrong, output empty record with "B" partition
        # empty_str = "," * (COMMON_EVENT_COLUMN_COUNT - 1)
        # return f"{empty_str}B".split(",")

        return [ None for i in range(COMMON_EVENT_COLUMN_COUNT - 1) ] + ['B']

def parse_json(line: str) -> str:
    """
    Takes json input line and turns into common_event record.
    Returns empty "bad" common record if anything goes wrong.

    Args: 
        line: json line from file
    Returns:
        common_event string record
    """ 

    try:
        
        # Turn records into dictionary and get common event of record
        record_dict = json.loads(line)

        bid_pr = None
        bid_size = None
        ask_pr = None
        ask_size = None
        trade_pr = None


        trade_dt = datetime.datetime.strptime(record_dict['trade_dt'], '%Y-%m-%d')
        symbol = record_dict['symbol']
        exchange = record_dict['exchange']
        event_tm = datetime.datetime.strptime(record_dict['event_tm'], '%Y-%m-%d %H:%M:%S.%f')
        event_seq_nb = int(record_dict['event_seq_nb'])
        rec_type = record_dict['event_type']
        arrival_tm = datetime.datetime.strptime(record_dict['file_tm'], '%Y-%m-%d %H:%M:%S.%f')
        partition = rec_type

        if rec_type == 'Q':
            bid_pr = Decimal(record_dict['bid_pr'])
            bid_size = int(record_dict['bid_size'])
            ask_pr = Decimal(record_dict['ask_pr'])
            ask_size = int(record_dict['ask_size'])
        else:
            trade_pr = Decimal(record_dict['price'])

        return [trade_dt, rec_type, symbol, exchange, event_tm, event_seq_nb, arrival_tm, \
            trade_pr, bid_pr, bid_size, ask_pr, ask_size, partition]
    
    except Exception as e:
    
        # If anything goes wrong, output empty record with "B" partition
        # empty_str = "," * (COMMON_EVENT_COLUMN_COUNT - 1)
        # return f"{empty_str}B".split(",")

        return [ None for i in range(COMMON_EVENT_COLUMN_COUNT - 1) ] + ['B']
