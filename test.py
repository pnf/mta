#!/usr/bin/env python

import os, sys, json, sys, time, csv, urllib2, pymongo
from rates import RateCalc
from collections import defaultdict

def av():
    return defaultdict(av)

sys.path.extend(['../protobuf-json-read-only','./pb'])

import protobuf_json

import nyct_subway_pb2 as nyct
import gtfs_realtime_pb2 as gtfs

# Read key from APIKEY
with open('APIKEY') as f:
    key = f.read()

while True:

    try:
        api = urllib2.urlopen(url='http://datamine.mta.info/mta_esi.php?key=' + key)
        # input = sys.stdin.read()
        input = api.read()
        
        fm = gtfs.FeedMessage()
        fm.ParseFromString(input)
        
        entity = fm.entity
        
        now = long(time.time())

        trips = av()

        for e in entity:

            #print e.id

            if e.HasField('vehicle'):
                trip_id = e.vehicle.trip.trip_id
                timestamp = e.vehicle.timestamp
                trips[trip_id]['timestamp'] = timestamp
                trips[trip_id]['vcount'] = trips[trip_id].get('vcount',0) + 1
                

            elif e.HasField('trip_update'):
                tu = e.trip_update
                trip_id = tu.trip.trip_id
                for a in tu.stop_time_update:
                    if not a.HasField('arrival'):
                        continue
                    stop_id = a.stop_id
                    ta = long(a.arrival.time)
                    trips[trip_id]['stop']=stop_id
                    trips[trip_id]['arrival'] = ta
                    trips[trip_id]['tcount'] = trips[trip_id].get('tcount',0) + 1
                    
                    break

        for i,t in trips.items():
                if t.get('tcount') == 1 and t.get('vcount') == 1:
                    continue
                print i, t.get('tcount','no tcount'), t.get('vcount', 'no vcount')

        break
                    

    except Exception as e:
        print >> sys.stderr, "Continuing after exception:",e

    time.sleep(60)
