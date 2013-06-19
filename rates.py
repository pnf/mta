#!/usr/bin/env python

# Input: now, trip_id, route_id, stop_id, eta, dt, stop_name
# Output: now, now_pretty, route_id, stop_id, seconds_ago_arrived, rate_per_tau

import os, sys, sys, time, csv, datetime, re, pymongo
from math import *

tau = 900. 	# seconds
adj = 1.0 - exp(-1.0)
rates = {}  	# "stop_id route_id" -> (rate, t, trip_id, t_pred)
arrived = {}	# "stop_id trip_id date" -> True

# TODO: start with latest now

from pymongo import MongoClient
client = MongoClient()
db = client.mta
etas = db.etas
ratedb = db.rates
meta = db.meta

# { "_id" : ObjectId("51c1fa225dfb8a0e45fbd815"), "route_id" : "1", "eta" : NumberLong(1371667060), "stop_id" : "121S", "now" : NumberLong(1371666978), "trip_id" : "084750_1..S01X500", "wait" : NumberLong(82) }

#reader = csv.reader(sys.stdin)
#for row in reader.

lastcalc = meta.find_one({'meta':'rates', 'tau': tau})
lastcalc = 0 if lastcalc is None else lastcalc.get('lastcalc',0)

for row in etas.find({'now' : {'$gte' : lastcalc}}).sort("now"):
    #if len(row)<7:
    # continue
    #(now, trip_id, route_id, stop_id, eta, dt, name) = row[:7]
    # print row
    (now, trip_id, route_id, stop_id, eta, dt) = [row[x] for x in ('now','trip_id','route_id','stop_id','eta','wait')]
    
    now = long(now)
    eta = long(eta)
    if eta<now-2*tau:  # An eta far in the past is suspicious
        continue
    route_id = route_id.strip()
    stop_id = stop_id.strip()
    trip_id = trip_id.strip()

    # Look out for bogus trip ids
    if not re.match("^\d{6}_\S{3}[SN]\d\d\w+",trip_id):
        continue

    key = route_id + ':' + stop_id
    key2 = key + ':' + trip_id + ':' + datetime.datetime.fromtimestamp(now).strftime('%Y%m%d')
    if key2 in arrived:
        continue

    if key in rates:
        (rate, t, prev_trip_id, prev_eta) = rates[key]
        # Is this a new trip_id?
        if trip_id != prev_trip_id:
            # Train arrived at previous eta
            arrived[key2] = True
            t_arrived = prev_eta
            rate = rate*exp(-(now-t)/tau) + 1.0*exp(-(now-t_arrived)/tau)
            rates[key] = (rate, now, trip_id, eta)
        else:
            # We have an updated eta
            rate = rate*exp(-(now-t)/tau)
            rates[key] = (rate, now, trip_id, eta)

        s_now = datetime.datetime.fromtimestamp(now).isoformat()
        s_prev = datetime.datetime.fromtimestamp(prev_eta).isoformat()

        m = re.match('.*T(\d\d):(\d\d):(\d\d)',s_now)
        if m:
            (hour,minute,second) = [int(x) for x in m.groups()]
            t_day = hour*3600 + minute*60 + second            

        # We have the rolling average including all past arrivals.
        # Calculate the rate as of the next eta (assuming it does arrive!)
        rate = rate*exp(-(eta-now)/tau) + 1.0
        if rate>1.0:
            rate = -1.0 / log(1.0 - 1.0/rate)
        rate = rate * 3600./tau
        # print "%d, %d, %s, %s, %s, %s, %d, %f" % (now, t_day,s_now, route_id, stop_id, s_prev,now-prev_eta, rate)
        ratedb.update({'now'       : now,
                       'route_id'  : route_id,
                       'stop_id'   : stop_id,
                       'tau'       : tau},
                      {'now'       : now,
                       't_day'     : t_day,
                       's_now'     : s_now,
                       'route_id'  : route_id,
                       'stop_id'   : stop_id,
                       's_prev'    : s_prev,
                       'since'     : now-prev_eta,
                       'rate'      : rate,
                       'tau'       : tau},
                      upsert=True)

    else:
        rates[key] = (0,now,trip_id,eta)

# Record last update
meta.update({'meta':'rates','tau':tau},
            {'meta':'rates','tau':tau, 'lastcalc':now},
            upsert=True)
