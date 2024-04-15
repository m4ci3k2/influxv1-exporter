#!/usr/bin/python3
from influxdb import InfluxDBClient
from influxdb.line_protocol import make_lines
from time import time
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
parser.add_argument('-C', '--gen-condition', action='append', default=None, metavar='field:value', help='generate condtions field=value and field:tag=value, useful when data in the database has mix of formats as influx will not return these rows at the same time')
parser.add_argument('-D', '--delete', action='store_true', help='delete data after reading')
parser.add_argument('--help', action='help')
args = parser.parse_args()

client = InfluxDBClient(args.host, args.port, args.user, args.password, args.db)
measurement = args.measurement

tag_result = client.query(f'SHOW TAG KEYS FROM "{ measurement }"')
tags = [i['tagKey'] for i in tag_result.get_points()]
tags_with_time = tags + ['time']

conditions = args.condition or []
for gen_condition in args.gen_condition or []:
    field, value = gen_condition.split(':')
    if value == None:
        raise RuntimeError("expected field:value in gen-condition")
    conditions += [f'{ field }={ value }', f'{ field }:tag={ value}']

fname = str(int(time()))+'.txt'
outfile = f = open(fname, 'w', encoding="utf-8")
print(f'saving to { fname }')

# one empty condition to make iteration correct if there are none
for condition in conditions or ['']:
    data_result = client.query(f'SELECT * FROM "{ measurement }" {condition}')

    for point in data_result.get_points():
        f.write(make_lines({'points': [{
            'measurement': measurement,
            'tags': { k: v for k, v in point.items() if k in tags},
            'fields': { k: v for k, v in point.items() if k not in tags_with_time},
            'time': point['time']
            }]
            }))
f.close()

if args.delete:
    print('saved; deleting data')
    for condition in conditions:
        client.query(f'DELETE FROM { measurement } {condition}')

print(f'now run `influx -import { fname } -pps 5000` to import data back')

