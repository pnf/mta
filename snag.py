#!/usr/bin/env python

import os, sys, json, sys, time, csv, urllib2, pymongo
from rates import RateCalc

sys.path.extend(['../protobuf-json-read-only','./pb'])

import protobuf_json

import nyct_subway_pb2 as nyct
import gtfs_realtime_pb2 as gtfs

from pymongo import MongoClient
client = MongoClient('localhost',3333)
db = client.mta
etas = db.etas
meta = db.meta

stop2name = {}
with open('static/stops.txt') as f:
    reader = csv.reader(f)
    for row in reader:
        stop = row[0]
        name = row[2]
        stop2name[stop] = name

# Read key from APIKEY
with open('APIKEY') as f:
    key = f.read()

r = RateCalc()
r.catchup()

while True:

    try:
        api = urllib2.urlopen(url='http://datamine.mta.info/mta_esi.php?key=' + key)
        # input = sys.stdin.read()
        input = api.read()
        
        fm = gtfs.FeedMessage()
        fm.ParseFromString(input)
        
        entity = fm.entity
        
        now = long(time.time())
        
        print >> sys.stderr, "snag: polling at",now
        etas.remove({'now':now})

        batch=[]

        for e in entity:
            if e.HasField('trip_update'):
                tu = e.trip_update
                trip_id = tu.trip.trip_id
                route_id = tu.trip.route_id
                # These come in order.  Keep only the first estimate that's in the future.
                for a in tu.stop_time_update:
                    stop_id = a.stop_id
                    ta = long(a.arrival.time)
                    wait = ta-now
                    print "%d, %s, %s, %s, %d, %d, %s" % (now, trip_id, route_id, stop_id, ta, ta-now, stop2name[stop_id])

                    batch.append({'now' : now,
                                 'trip_id' : trip_id,
                                 'route_id' : route_id,
                                 'stop_id' : stop_id,
                                 'eta' : ta,
                                 'wait' : ta-now})
                    r.process(now,trip_id,route_id,stop_id,ta,wait)
                    if wait>0:
                        break

        if len(batch)>0:
            print >>sys.stderr, "snag: inserting",len(batch),"records at",now
            etas.insert(batch)
            r.write(None)  # Make sure we're caught up in rates db too

        meta.update({'meta':'snag'},{'meta':'snag', 'lastsnag':now}, upsert=True)
                    

    except Exception as e:
        print >> sys.stderr, "Continuing after exception:",e

    time.sleep(60)
