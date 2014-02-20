#!/usr/bin/env python

# input: static_trip_id, arrived, departed, stop_id, ...
# static_trip_id deocded as
# A20111204SAT_021150_2..N08R is decoded as follows:
# A                           sub-Division identifier. A=IRT, B=BMT+IND
#  20111204                   schedule effective date
#          SAT                service code. Typically it will be WKD-Weekday, SAT-Saturday or SUNSunday
#              021150         *origin time in 0.01s past midnightn
#                     2..     *route id
#                        N    *direction
#                         08R path identifier
# The *'d fields define trip uniquely
# arrival and depart are in local time


import os, sys, sys, time, csv, datetime, re, pymongo
from math import *

tau = 900. 	# seconds
rates = {}  	# "stop_id route_id" -> (rate, t, trip_id)
done = {}       # keys for which we are done

from pymongo import MongoClient
port = int(os.environ.get('MONGO_PORT',3333))
host = os.environ.get('MONGO_HOST','localhost')
client = MongoClient(host,port)
db = client.mta
sched = db.sched

# empty
sched.remove()

reader = csv.reader(sys.stdin)

# For loading the main sched collection
n_bulk = 1000
batch = []
n = 0

# (stop_id, route_id) => seq
seqs = {}


def accrue_rate(stop_id, route_id,service_code, arrived):
    key  = (stop_id, route_id,service_code)

    if key in done:
        return (None,None,None)
    else:
        s_arrived = arrived
        m = re.match('(\d\d):(\d\d):(\d\d)',arrived)
        if not m:
            return (None,None,None)
        else:
            (hour,minute,second) = [int(x) for x in m.groups()]
            arrived = hour*3600 + minute*60 + second
            now = arrived
            if key in rates:
                (rate, t, prev_trip_id, prev_arrived) = rates[key]
                if prev_arrived > now:
                    done[key] = True
                    return (None,None,None)
                rate = rate*exp(-(arrived-prev_arrived)/tau) + 1.0
            else:
                rate = 1.0
                prev_arrived = now
            rates[key] = (rate,arrived, trip_id, arrived)
            # Correct for discreteness
            if rate>1.0:
                rate = -1.0 / log(1.0 - 1.0/rate)
                # Scale to hourly
                rate = rate * 3600/tau
            return (rate,now,prev_arrived)


# We're counting on effective date being in decreasing order, arrival times in increasing order
# We'll keep two sets of rates, one for platform and one for platform + route
for row in reader:
    if len(row)<4:
        continue
    (trip_id, arrived, departed, stop_id,stop_sequence) = row[:5]
    m = re.match('([AB])(\d{8})(\w{3})_(\d{6})_(\w+)\.*([NS]).*', trip_id)
    if not m:
        continue

    (_,eff_date,service_code,origin_time,route_id,direction) = m.groups()

    (rate,now,prev_arrived) = accrue_rate(stop_id,route_id,service_code,arrived)
    if rate:
            batch.append({'now' 		: now,
                  't_day'		: now,
                  's_arrived'		: arrived,
                  'service_code'	: service_code,
                  'route_id'		: route_id,
                  'eff_date'		: eff_date,
                  'stop_id'		: stop_id,
                  'since'		: now-prev_arrived,
                  'rate'		: rate,
                  'tau'			: tau})

    (rate,now,prev_arrived) = accrue_rate(stop_id,'*',service_code,arrived)
    if rate:
            batch.append({'now' 		: now,
                  't_day'		: now,
                  's_arrived'		: arrived,
                  'service_code'	: service_code,
                  'route_id'		: '*',
                  'eff_date'		: eff_date,
                  'stop_id'		: stop_id,
                  'since'		: now-prev_arrived,
                  'rate'		: rate,
                  'tau'			: tau})

    if len(batch)>n_bulk:
        sched.insert(batch)
        n = n + len(batch)
        batch = []
        print >> sys.stderr,"Inserted",n,"records"

    combo = (stop_id, route_id, direction)
    if not combo in seqs:
        seqs[combo] = stop_sequence
        
if len(batch)>0:
    sched.insert(batch)
    pass

route2stop = {}
batch = []
for stop_id, route_id, direction in seqs.keys():
    route2stop[(route_id,direction)] = []
for stop_id, route_id, direction in seqs.keys():
    stop_sequence = seqs[(stop_id,route_id,direction)]
    route2stop[(route_id,direction)].append((stop_id,stop_sequence))
for route_id, direction in route2stop.keys():
    stops = route2stop[(route_id,direction)]
#    stops.sort(cmp=lambda a,b: int(a[1])-int(b[1]))
    stops.sort(key=lambda x: x[1])
    batch.append({'route_id':route_id,
                  'direction':direction,
                  'stops':[x[0] for x in stops]})
db.routestops.remove()
db.routestops.insert(batch)

    # sched.update({'now'			: now,
    #               'route_id'		: route_id,
    #               'stop_id'		: stop_id,
    #               'service_code'	: service_code},
    #               {'now' 		: now,
    #               's_arrived'		: s_arrived,
    #               'service_code'	: service_code,
    #               'route_id'		: route_id,
    #               'eff_date'		: eff_date,
    #               'stop_id'		: stop_id,
    #               'since'		: now-prev_arrived,
    #               'rate'		: rate,
    #               'tau'			: tau},
    #              upsert=True)

    # print "%d, %s, %s, %s, %s, %d, %f" % (now, s_arrived, service_code, route_id, stop_id, now-prev_arrived, rate)
