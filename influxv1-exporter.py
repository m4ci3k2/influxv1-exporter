#!/usr/bin/python3
from influxdb import InfluxDBClient
from influxdb.line_protocol import make_lines
import argparse

__license__ = 'GPL-3.0'
parser = argparse.ArgumentParser(description='Dumps data from influx series separating tags and fields and writes influx line format. If the same name is sometimes tag and sometimes field --- tag wins.', add_help=False)
parser.add_argument('-h', '--host', default='localhost')
parser.add_argument('-P', '--port', default=8086)
parser.add_argument('-u', '--user', default=None)
parser.add_argument('-p', '--password', default=None)
parser.add_argument('-d', '--db', default='NOAA_water_database')
parser.add_argument('-m', '--measurement', default='h2o_feet')
parser.add_argument('-c', '--condition', action='append')
args = parser.parse_args()

client = InfluxDBClient(args.host, args.port, args.user, args.password, args.db)
measurement = args.measurement

tag_result = client.query('SHOW TAG KEYS FROM ' + measurement)
tags = [i['tagKey'] for i in tag_result.get_points()]
tags_with_time = tags + ['time']

conditions = [''] if not args.condition else args.condition
for condition in conditions:
    data_result = client.query(f'SELECT * FROM { measurement } {condition}')

    for point in data_result.get_points():
        print(make_lines({'points': [{
            'measurement': measurement,
            'tags': { k: v for k, v in point.items() if k in tags},
            'fields': { k: v for k, v in point.items() if k not in tags_with_time},
            'time': point['time']
            }]
            }))
