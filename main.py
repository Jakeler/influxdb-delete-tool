from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet

from prompt_toolkit import prompt, print_formatted_text as fprint, HTML
from prompt_toolkit.completion import WordCompleter


def resp_list(input: dict):
    return [x["name"] for x in input]

def select_db(client: InfluxDBClient):
    databases = resp_list(client.get_list_database())
    fprint(databases)

    comp = WordCompleter(databases)
    db = prompt(HTML('Which <ansicyan>database</ansicyan> do you want to work on today? '), 
        completer=comp, complete_while_typing=True)
    if (db not in databases):
        fprint(HTML(f'<ansired>DB {db} does not exist, aborting!</ansired>'))
        return False

    client.switch_database(db)
    return True

def select_msm(client: InfluxDBClient):
    rs: ResultSet = dbc.query('SHOW measurements')
    msms = resp_list(list(rs.get_points()))
    print(msms)

    comp = WordCompleter(msms)
    msm = prompt(HTML('And what <ansicyan>measurement</ansicyan> contains crap? '), 
        completer=comp, complete_while_typing=True)
    return msm


# TODO parse args for hostname, port, user, ask pass
dbc = InfluxDBClient('homeserver')
select_db(dbc)
measurement = select_msm(dbc)
