import signal
import csv

from typing import Any

#### Other Utilities ##############################################################################

def encodeSpecialChars(p_in):
    '''convert special chars to escaped representation'''
    buff=[]
    for line in p_in:
        newLine=[]
        for col in line:
            if isinstance(col, str):
                newData=[]
                for b in col:
                    i=ord(b)
                    if i<32:
                        newData.append(r'\{hex(i)}')
                    else:
                        newData.append(b)
                newLine.append(''.join(newData))
            else:
                newLine.append(col)
        buff.append(tuple(newLine))
    return tuple(buff)

def identify_type(obj):
    '''Identifies the type of an object as integer, float, date, or string.

    Args:
        obj: The object to identify the type of.

    Returns:
        A string indicating the type of the object.
    '''
    t = type(obj)
    if t == int:
        return 'integer'
    elif t == float:
        return 'float'
    elif str(t).startswith("<class 'datetime."):  # Replace with your date library if needed
        return 'date'
    elif t == str:
        return 'string'
    else:
        return 'unknown'

def delimiter_decoder(delimiter_name:str) -> str:
    delim_decoder = {
    'tab': '\t',
    'space': ' ',
    'comma': ',',
    'colon': ':',
    'semicolon': ';',
    'newline': '\n',
    'pipe': '|',
    'slash': '/',
    'backslash': '\\',
    'dollar': '$',
    'at': '@',
    'percent': '%',
    'hash': '#',
    'ampersand': '&',
    'equals': '=',
    'plus': '+',
    'minus': '-',
    'asterisk': '*',
    'lparen': '(',
    'rparen': ')',
    'lbracket': '[',
    'rbracket': ']',
    'lbrace': '{',
    'rbrace': '}',
    'lt': '<',
    'gt': '>',
    'tilde': '~',
    'exclamation': '!',
    'question': '?',
    'dot': '.',
    'underscore': '_',
    'hiphen': '-',
    'backtick': '`',
    'doublequote': '"',
    'singlequote': "'",
    }

    try:
        return delim_decoder[delimiter_name]
    except:
        return delimiter_name

def block_signals():
    '''we want to make sure these threads exit gracefully, only when shared.Working.value is False'''
    signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGINT])
    signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGTERM])

    #the previous lines don't seem to fix everything, also trying to ignore the signals
    signal.SIG_IGN
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


def convert_str2value(value: str, emptyValuesDefault=None) -> Any:
    """Converts a string value to an appropriate data type (int, float, bool)."""
    if value.lower() in ['true', 'false']:
        return value.lower() == 'true'
    if value.isdigit():
        return int(value)
    if value.replace('.', '', 1).isdigit():  # Check for float
        return float(value)
    if value == '':
        return emptyValuesDefault
    return value

def read_csv_config(filename, *, emptyValuesDefault=None, sequencialLineNumbers:bool=False, sequencialStartsAt:int=1) -> dict[int, dict[str, Any]]:
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter='\t')
        data:dict[int, Any] = {}
        realLineNumber = 0
        sequencialLineNumber = sequencialStartsAt
        headers = None

        for row in reader:
            realLineNumber += 1

            # Skip empty lines or lines where the first field starts with an #
            if not row or row[0].startswith('#'):
                continue

            # If headers are not set, use the first non-commented line as headers
            if headers is None:
                headers = row
                continue

            # Create a dictionary for the current row
            rowDict = {headers[i]: convert_str2value(value=cell, emptyValuesDefault=emptyValuesDefault) for i, cell in enumerate(row)}
            data[sequencialLineNumber if sequencialLineNumbers else realLineNumber] = rowDict
            sequencialLineNumber += 1

    return data

def seconds_to_readable(seconds:float) -> str:
        minutes = seconds // 60
        hours = minutes // 60
        days = hours // 24
        minutes %= 60
        hours %= 24
        seconds %= 60
        if days > 0:
            formatted = f'{round(days)}d {round(hours)}h {round(minutes)}m {round(seconds)}s'
        elif hours > 0:
            formatted = f'{round(hours)}h {round(minutes)}m {round(seconds)}s'
        elif minutes > 0:
            formatted = f'{round(minutes)}m {round(seconds)}s'
        else:
            formatted = f'{seconds:,.2f} seconds'

        return formatted