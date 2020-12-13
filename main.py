from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet
from influxdb.exceptions import InfluxDBClientError

from prompt_toolkit import prompt, print_formatted_text as fprint, HTML
from prompt_toolkit.completion import WordCompleter


def resp_list(input: dict, key = 'name'):
    return [x[key] for x in input]

def color_print(text: str, color: str):
    fprint(HTML(f'<{color}>{text}</{color}>'))

def select_db(client: InfluxDBClient):
    databases = resp_list(client.get_list_database())
    fprint(databases)

    comp = WordCompleter(databases)
    db = prompt(HTML('Which <ansicyan>database</ansicyan> do you want to work on today?\n'), 
        completer=comp, complete_while_typing=True)
    if (db not in databases):
        color_print(f'DB "{db}" does not exist, try to remember! (or hit TAB)', 'ansired')
        return False

    client.switch_database(db)
    return True

def select_msm(client: InfluxDBClient):
    rs: ResultSet = dbc.query('SHOW measurements')
    msms = resp_list(list(rs.get_points()))
    print(msms)

    comp = WordCompleter(msms)
    msm = prompt(HTML('And what <ansicyan>measurement</ansicyan> contains crap (you can only choose one)?\n'), 
        completer=comp, complete_while_typing=True)
    return msm

def get_condition(client: InfluxDBClient, msm: str):
    # TODO mode for mutli measurements
    rs: ResultSet = dbc.query(f'SHOW TAG KEYS FROM {msm}')
    tags = resp_list(list(rs.get_points()), 'tagKey')
    print(tags)
    rs: ResultSet = dbc.query(f'SHOW FIELD KEYS FROM {msm}')
    fields = resp_list(list(rs.get_points()), 'fieldKey')
    print(fields)

    comp = WordCompleter(['time'] + tags + fields)
    condition = prompt(HTML('Please choose a <ansicyan>condition WHERE</ansicyan> it is wrong:\n'), 
        completer=comp, complete_while_typing=True)
    return condition

def ask_confirm():
    answer = prompt('Do you really want to delete this? y/N')
    return answer in ('y', 'Y', 'yes', 'Yes')

def table_print(input: [dict]):
    header = input[0].keys()
    fprint(HTML(f"<u>{'  '.join(header)}</u>"))
    for line in input:
        fprint('  '.join(str(x) for x in line.values()))

# TODO parse args for hostname, port, user, ask pass
dbc = InfluxDBClient('homeserver')

db_selected = False
while not db_selected:
    db_selected = select_db(dbc)
measurement = select_msm(dbc)

okay = False
while not okay:
    cond = get_condition(dbc, measurement)

    try:
        rs: ResultSet = dbc.query(f'SELECT * FROM {measurement} WHERE {cond}')
        entries = (list(rs.get_points()))
        n = len(entries)
        if n < 1:
            color_print('That makes no sense man: query results in zero entries!', 'ansired')
            continue

        fprint(HTML(f'Found <b>{n}</b> candidates for deletion:'))
        table_print(entries)

        okay = ask_confirm()
    except InfluxDBClientError as err:
        color_print(f'Database computer says: ' + err.content, 'ansired')
        entries = []
        okay = False



