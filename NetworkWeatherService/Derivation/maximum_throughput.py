
# TODO: Keep a dictionary of source-dest pairs => bools to keep track of
# the source-dest pairs we've already computed maximum throughput for.

def max_throughput(time_a, time_b, mean_packet_loss):
    # Expected TCP segment size limit: 1500 octets
    mean_segment_size = 1500
    round_trip_time = time_a + time_b
    # Formula given by https://en.wikipedia.org/wiki/TCP_tuning#Packet_loss
    return mean_segment_size / (round_trip_time * math.sqrt(mean_packet_loss))

nw_index = "network_weather-2015-10-11"

def get_all_throughputs():
    for s in usrcs:
        for d in udests:
            st={
            "query": {
                    "filtered":{
                        "query": {
                            "match_all": {}
                        },
                        "filter":{
                            "and": [
                                {
                                    "term":{ "@message.src":s }
                                },
                                {
                                    "term":{ "@message.dest":d }
                                }
                            ]
                        }
                    }
                }
            }

            st_rev={
            "query": {
                    "filtered":{
                        "query": {
                            "match_all": {}
                        },
                        "filter":{
                            "and": [
                                {
                                    "term":{ "@message.src":d }
                                },
                                {
                                    "term":{ "@message.dest":s }
                                }
                            ]
                        }
                    }
                }
            }

            res = es.search(index=nw_index, body=st, size=1000)
            res_rev = es.search(index=nw_index, body=st_rev, size=1000)

            if res and res_rev:
                # Both source-dest and dest-source result dictionaries are nonempty
                num_sd_delay = 0
                tot_sd_delay = 0
                num_ds_delay = 0
                tot_ds_delay = 0
                num_pl = 0
                tot_pl = 0

                # Calculate simple averages
                for sd_hit in res['hits']['hits']:
                    if sd_hit['_type'] == 'packet_loss_rate':
                        num_pl += 1
                        tot_pl += sd_hit['_source']['@message']['packet_loss']
                    elif sd_hit['_type'] == 'latency':
                        num_sd_delay += 1
                        tot_sd_delay += sd_hit['_source']['@message']['delay_mean']

                for ds_hit in res_rev['hits']['hits']:
                    if ds_hit['_type'] == 'packet_loss_rate':
                        num_pl += 1
                        tot_pl += ds_hit['_source']['@message']['packet_loss']
                    elif sd_hit['_type'] == 'latency':
                        num_ds_delay += 1
                        tot_ds_delay += ds_hit['_source']['@message']['delay_mean']

                avg_sd_delay = tot_sd_delay / num_sd_delay
                avg_ds_delay = tot_ds_delay / num_ds_delay
                avg_pl = tot_pl / num_pl

                tp = max_throughput(avg_sd_delay, avg_ds_delay, avg_pl)
                print 'max throughput for source-destination pair (%s - %s):\t %f' % (s, d, tp)
