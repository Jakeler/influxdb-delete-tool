from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet
from influxdb.exceptions import InfluxDBClientError
INFLUX_DOC_URL = 'https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#the-where-clause'
LARGE_THRESHOLD = 100

from prompt_toolkit import prompt, PromptSession, print_formatted_text as fprint, HTML
from prompt_toolkit.shortcuts import button_dialog
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import FuzzyWordCompleter

import sys, argparse


def resp_list(input: dict, key = 'name'):
    return [x[key] for x in input]

def color_print(text: str, color: str, extra: str = ''):
    fprint(HTML(f'<{color}>{text}</{color}>{extra}'))

def found_print(name: str, items: list):
    color_print(f'Okay, there are some {name}: ', 'ansigreen', str(items))

def select_db(client: InfluxDBClient):
    databases = resp_list(client.get_list_database())
    found_print('databases', databases)

    comp = FuzzyWordCompleter(databases)
    db = prompt(HTML('Which <ansicyan>database</ansicyan> do you want to work on today?\n'), 
        completer=comp, complete_while_typing=True)
    if (db not in databases):
        color_print(f'DB "{db}" does not exist, try to remember! (or hit TAB)', 'ansired')
        return False

    client.switch_database(db)
    return True

def select_msm(client: InfluxDBClient):
    rs: ResultSet = client.query('SHOW measurements')
    msms = resp_list(list(rs.get_points()))
    found_print('measurements', msms)

    comp = FuzzyWordCompleter(msms)
    msm = prompt(HTML('And what <ansicyan>measurement</ansicyan> contains crap (you can only choose one)?\n'), 
        completer=comp, complete_while_typing=True)
    return msm

def get_condition_session(client: InfluxDBClient, msm: str):
    # TODO mode for mutli measurements
    rs: ResultSet = client.query(f'SHOW TAG KEYS FROM {msm}')
    tags = resp_list(list(rs.get_points()), 'tagKey')
    found_print('tags', tags)
    rs: ResultSet = client.query(f'SHOW FIELD KEYS FROM {msm}')
    fields = resp_list(list(rs.get_points()), 'fieldKey')
    found_print('fields', fields)

    comp = FuzzyWordCompleter(['time'] + tags + fields)
    session = PromptSession(HTML('Please choose a <ansicyan>condition WHERE</ansicyan> it is wrong:\n'),
        completer=comp, complete_while_typing=True, auto_suggest=AutoSuggestFromHistory())
    return session

def get_count(client: InfluxDBClient, msm: str, cond: str):
    rs: ResultSet = client.query(f'SELECT COUNT(*) FROM {msm} WHERE {cond}')
    result = list(rs.get_points())
    print(result)
    return result[0]['count_pcs']

def get_results(client: InfluxDBClient, msm: str, cond: str) -> int:
    rs: ResultSet = client.query(f'SELECT * FROM {msm} WHERE {cond}', epoch='ns')
    return list(rs.get_points())

def ask_confirm():
    answer = prompt('Do you really want to delete this selection? y/N ')
    return answer in ('y', 'Y', 'yes', 'Yes')

def ask_large(n: int) -> bool:
    return button_dialog(
        title=f'High result count ({n})',
        text=f'You are trying to get {n} points, can this be correct? It could take long to load and execute...',
        buttons=[
            ('YES', True),
            ('No (edit)', False),
        ],
    ).run()

def table_print(input: [dict]):
    header = input[0].keys()
    fprint(HTML(f"<u>{'  '.join(header)}</u>"))
    for line in input:
        fprint('  '.join(str(x) for x in line.values()))
    print()

def delete_entries(client: InfluxDBClient, msm: str, entries: list):
    for row in entries:
        t = row["time"]
        rs: ResultSet = client.query(f"DELETE FROM {msm} WHERE time = {t}")
        color_print(f'DELETED {t}', 'ansiyellow', f' result {list(rs.get_points())}')

def run_main(args: argparse.Namespace):
    color_print(f"Connecting with paramters {vars(args)}\n", 'ansigray')
    dbc = InfluxDBClient(**vars(args))

    db_selected = False
    while not db_selected:
        db_selected = select_db(dbc)
    measurement = select_msm(dbc)

    okay = False
    session = get_condition_session(dbc, measurement)
    while not okay:
        cond = session.prompt()
        print('Query started...')

        try:
            entries = get_results(dbc, measurement, cond)
            n = len(entries)
            if n > LARGE_THRESHOLD:
                if not ask_large(n):
                    continue

            if n < 1:
                color_print('That makes no sense dude: query results in zero entries!', 'ansired')
                color_print('RTFM: ' + INFLUX_DOC_URL, 'ansired')
                continue

            color_print(f'Found <b>{n}</b> candidates for deletion:', 'ansigreen')
            table_print(entries)

            okay = ask_confirm()
        except InfluxDBClientError as err:
            color_print(f'Database computer says: ' + err.content, 'ansired')
            color_print('RTFM: ' + INFLUX_DOC_URL, 'ansired')
            entries = []
            okay = False

    # Here we decided to do it
    delete_entries(dbc, measurement, entries)
    color_print('Mission accomplished!', 'ansigreen')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(  
        description='IDT: tool to delete points with bad field values in InfluxDB',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)  
    
    parser.add_argument('host', nargs='?', default='localhost', help='hostname of DB instance')
    parser.add_argument('-p', '--port', required=False, default=8086, help='port of DB instance')
    parser.add_argument('-u', '--username', required=False, help='username with suitable rights')
    parser.add_argument('-x', '--password', required=False, help='password for user authentication')
    parser.add_argument('-s', '--ssl', required=False, action='store_true', help='use HTTPS instead of plain HTTP')
    args = parser.parse_args()

    try:
        run_main(args)
    except KeyboardInterrupt as kir:
        color_print('KeyboardInterrupt received, aborting mission!', 'ansired')
        sys.exit(0)



