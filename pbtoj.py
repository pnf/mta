#!/usr/bin/python
import os, sys, json,sys
from pprint import pprint
sys.path.extend(['../protobuf-json-read-only','./pb'])
import protobuf_json
import nyct_subway_pb2 as nyct
import gtfs_realtime_pb2 as gtfs
input = sys.stdin.read()
fm = gtfs.FeedMessage()
fm.ParseFromString(input)
json = protobuf_json.pb2json(fm)
print json

