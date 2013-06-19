#!/usr/bin/python

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


import os, sys, sys, time, csv, datetime, re
from math import *

tau = 900. 	# seconds
rates = {}  	# "stop_id route_id" -> (rate, t, trip_id)
done = {}       # keys for which we are done

reader = csv.reader(sys.stdin)

# We're counting on effective date being in decreasing order, arrival times in increasing order

for row in reader:
    if len(row)<4:
        continue
    (trip_id, arrived, departed, stop_id) = row[:4]
    m = re.match('([AB])(\d{8})(\w{3})_(\d{6})_(\w+)\.*([NS]).*', trip_id)
    if  not m:
        continue
    (_,eff_date,service_code,origin_time,route_id,direction) = m.groups()
    key = stop_id + " " + route_id
    if key in done:
        continue
    dt = arrived
    m = re.match('(\d\d):(\d\d):(\d\d)',arrived)
    if not m:
        continue
    (hour,minute,second) = [int(x) for x in m.groups()]
    arrived = hour*3600 + minute*60 + second
    now = arrived
    if key in rates:
        (rate, t, prev_trip_id, prev_arrived) = rates[key]
        if prev_arrived > now:
            done[key] = True
            continue
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
    print "%d, %s, %s, %s, %d, %f" % (now, dt, route_id, stop_id, now-prev_arrived, rate)
