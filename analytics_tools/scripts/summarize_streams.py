#!/usr/bin/env python
"""
Outputs the a time range's event counts, payloads, and schemas for each stream.



Usage:
  python scripts/summarize_streams.py --kronos-url http://your-kronos-instance:port
                                     [--csv myfile.csv]
                                     [--start 2003-09-25T10:49:41.5-03:00]
                                     [--end 2003-09-25T10:49:41.5-03:00]

If not supplied, --start defaults to 24 hours before --end, and --end defaults
to now.
"""
import argparse
import csv
import datetime
import numpy as np
import sys
import ujson
from dateutil.parser import parse
from dateutil.tz import tzlocal
from pykronos import KronosClient
from pykronos.common.time import datetime_to_kronos_time

output = """
Stream: %(stream)s

Event count:
  Total:      %(total_events)d
  Per day:    %(events_per_day)f
  Per second: %(events_per_sec)f

Payload (bytes):
  Total:  %(payload_total_bytes)d
  Avg:    %(payload_avg_bytes)f
  Median: %(payload_med_bytes)d
  95th:   %(payload_95_bytes)d
  99th:   %(payload_99_bytes)d

Schema:
  %(schema)s

"""

def indent(msg, indent):
  msg = msg.splitlines()
  for line in msg:
    sys.stdout.write('%s%s\n' % (' ' * indent, line))

def main(args):
  client = KronosClient(args.kronos_url)
  headers = [
    'stream',
    'total_events',
    'events_per_day',
    'events_per_sec',
    'payload_total_bytes',
    'payload_avg_bytes',
    'payload_med_bytes',
    'payload_95_bytes',
    'payload_99_bytes',
    'schema',
  ]
  if args.csv:
    csv_file = open(args.csv, 'w')
    writer = csv.DictWriter(csv_file, headers)
    writer.writeheader()
  else:
    print '-' * 79
  for stream in client.get_streams():
    total_events = 0
    payloads = []
    for event in client.get(stream, args.start, args.end):
      payloads.append(len(ujson.dumps(event)))
      total_events += 1
    if total_events == 0:
      indent('%s has no events' % stream, 2)
      print '-' * 79
      continue
    timeframe_sec = (args.end - args.start).total_seconds()
    schema = client.infer_schema(stream)['schema']
    context = dict(zip(headers, [
      stream,
      total_events,
      (float(total_events) / timeframe_sec) * 60 * 60 * 24,
      float(total_events) / timeframe_sec,
      np.sum(payloads),
      np.mean(payloads),
      np.median(payloads),
      np.percentile(payloads, 95),
      np.percentile(payloads, 99),
      schema,
    ]))
    if args.csv:
      writer.writerow(context)
    else: 
      indent(output % context, 2)
      print '-' * 79

def process_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--kronos-url', required=True,
                      help='The Kronos server to retrieve data from')
  parser.add_argument('--csv', help='A CSV file for output')
  parser.add_argument('--start',
                      help=('When to start retrieving? (format: '
                            '2003-09-25T10:49:41.5-03:00)'))
  parser.add_argument('--end',
                      help=('When to end retrieving? (format: '
                            '2003-09-25T10:49:41.5-03:00)'))  
  args = parser.parse_args()
  if args.end:
    args.end = parse(args.end)
  else:
    args.end = datetime.datetime.utcnow()
  if args.start:
    args.start = parse(args.start)
  else:
    args.start = args.end - datetime.timedelta(hours=24)
  if args.end < args.start:
    sys.stderr.write('ERROR: --start must be earlier than --end\n')
    exit(1)
  if (args.end - args.start).total_seconds() < 1:
    sys.stderr.write('ERROR: timeframe must be at least 1 second\n')
    exit(1)
  return args

if __name__ == '__main__':
  main(process_args())
