#!/usr/bin/env python

# Input: now, trip_id, route_id, stop_id, eta, dt, stop_name
# Output: now, now_pretty, route_id, stop_id, seconds_ago_arrived, rate_per_tau

import os, sys, sys, time, csv, datetime, re, pymongo
from pymongo import MongoClient
from math import *

class RateCalc:
    def __init__(this,hostname='localhost',port=3333,tau=900.):
        this.client = MongoClient(hostname,port)
        this.tau = tau
        this.rates = {}         # "stop_id route_id" -> (rate, t, trip_id, t_pred)
        this.arrived = {}       # "stop_id trip_id date" -> True
        db = this.client.mta
        this.etas = db.etas
        this.ratedb = db.rates
        this.meta = db.meta
        this.n_batch = 1000
        this.n=0
        this.batch = []

    def write(this,row=None):
        if row is not None:
            this.batch.append(row)
        if row is None or len(this.batch)>this.n_batch:
            this.ratedb.insert(this.batch)
            this.n = this.n + len(this.batch)
            print >> sys.stderr,"rates: inserted",this.n,"records, for",this.batch[0]['now']
            this.batch = []

    def catchup(this,extra=0):
        this.process_stream(this.catchup_iter())

    def catchup_iter(this, extra=0):
        lastcalc = this.ratedb.find().sort([('now',pymongo.DESCENDING)]).limit(1)
        lastcalc = lastcalc[0]['now'] if lastcalc.count()>0 else 0
        lastcalc = lastcalc - extra
        # Clear out everything exactly at last time, and recalculate
        this.ratedb.remove({'now':{'$gte' : lastcalc}})
        warmup = lastcalc - 24*3600
        print >> sys.stderr, "rates: catching up.  now=",long(time.time())," lastcalc=",lastcalc,"warmup=",warmup
        for row in this.etas.find({'now' : {'$gte' : warmup}},sort=[('now',pymongo.ASCENDING)]):
            (now, trip_id, route_id, stop_id, eta, dt) = [row[x] for x in ('now','trip_id','route_id','stop_id','eta','wait')]
            yield        (now, trip_id, route_id, stop_id, eta, dt, now>=lastcalc)
            #this.process(now, trip_id, route_id, stop_id, eta, dt, now>=lastcalc)

    def process_stream(this, etas):
        for now, trip_id, route_id, stop_id, eta, dt, do_write in etas:
            this.process(now, trip_id, route_id, stop_id, eta, dt, do_write)
            this.process(now, trip_id, '*', stop_id, eta, dt, do_write)
        this.write()

    def process(this,now, trip_id, route_id, stop_id, eta, dt,do_write=True):
        now = long(now)
        eta = long(eta)
        if eta<now-2*this.tau:  # An eta far in the past is suspicious
            return
        route_id = route_id.strip()
        stop_id = stop_id.strip()
        trip_id = trip_id.strip()
        # Look out for bogus trip ids
        if not re.match("^\d{6}_\S{3}[SN]\d\d\w+",trip_id):
            return

        key = (route_id, stop_id)
        key2 = (key,trip_id, datetime.datetime.fromtimestamp(now).strftime('%Y%m%d'))
        if key2 in this.arrived:
            return
        t_arrived=0
        prev_trip_id = 'n/a'
        if key not in this.rates:
            this.rates[key] = (0,now,trip_id,eta)
        else:
            (rate, t, prev_trip_id, prev_eta) = this.rates[key]
            if trip_id != prev_trip_id:
                # Train actually arrived at previous eta
                this.arrived[key2] = True
                t_arrived = prev_eta
                rate = rate*exp(-(now-t)/this.tau) + 1.0*exp(-(now-t_arrived)/this.tau)
                this.rates[key] = (rate, now, trip_id, eta)
                status = 'arrived'
            else:
                # We have an updated eta
                rate = rate*exp(-(now-t)/this.tau)
                this.rates[key] = (rate, now, trip_id, eta)
                status = 'eta'

            s_now = datetime.datetime.fromtimestamp(now).isoformat()
            s_prev = datetime.datetime.fromtimestamp(prev_eta).isoformat()
            #print >>sys.stderr,now, s_now
            m = re.match('(.*)T(\d\d):(\d\d):(\d\d)',s_now)
            if m:
                (day,hour,minute,second) = m.groups()
                t_day = int(hour)*3600 + int(minute)*60 + int(second)

            # We have the rolling average including all past arrivals.
            # Calculate the rate as of the next eta (assuming it does arrive!)
            rate = rate*exp(-(eta-now)/this.tau) + 1.0
            if rate>1.0:
                rate = -1.0 / log(1.0 - 1.0/rate)
                rate = rate * 3600./this.tau

            if do_write:
                this.write({'now'       : now,
                            'day'       : day,
                            't_day'     : t_day,
                            's_now'     : s_now,
                            'route_id'  : route_id,
                            'stop_id'   : stop_id,
                            'trip_id'   : trip_id,
                            'prev_trip_id' : prev_trip_id,
                            's_prev'    : s_prev,
                            'arrived'   : t_arrived,
                            'since'     : now-t_arrived,
                            'eta'             : eta,
                            'until'     : eta - now,
                            'rate'      : rate,
                            'status'    : status,
                            'tau'       : this.tau})

if __name__ == "__main__":
    r = RateCalc()
    r.catchup()
