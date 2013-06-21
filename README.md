mta
===

Static data lives here


     http://www.mta.info/developers/data/nyct/subway/google_transit.zip


Dynamic data




Basic format of the protobuf is:


        entity[ ].id
                   ?vehicle.timestamp  # time of last detected movement
                           .current_stop_sequence
                           .trip.nyct_trip_descriptor.direction,is_assigned,train_id
                                .route_id, trip_id, current_status, stop_id
                   ?alert
                   ?trip_update.id
                               .stop_time_update[i].arrival.time
                                                    .nyct_stop_time_update
                                                    ?departure
                                                    .stop_id  # e.g. 132N is 14th st northbound
                               .trip.route_id   # line
                                    .trip_id  # uid for whole trip
                                    .start_date
                                    .nyct_trip_descriptor.direction,is_assigned,train_id


What I want to get is the series of arrival times at for each route/stop combination.
The id is only unique within a message.  I believe the trip_id is unique and equivalent to
a particular nyct_trip_descriptor.  
Since there is no historical data, I must detect that a station has been passed by a
particular trip_id.

So: accumulate (trip_id, route_id, stop_id, arrival.time)
arrival.time should remain roughly constant, and arrival.time-now() roughly diminishes
When the trip_id x stop_id x route_id drops off capture most recent arrival time.

