from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet

def resp_list(input: dict):
    return [x["name"] for x in input]


# TODO parse args for hostname, port, user, ask pass
dbc = InfluxDBClient('homeserver')

databases = resp_list(dbc.get_list_database())
print(databases)

dbc.switch_database('env')

rs: ResultSet = dbc.query('SHOW measurements')
measures = list(rs.get_points())
print(resp_list(measures))