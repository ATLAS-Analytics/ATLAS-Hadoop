REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/import/elasticsearch-hadoop-5.1.2/dist/elasticsearch-hadoop-pig-5.1.2.jar'

define EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=http://atlas-kibana.mwt2.org:9200','es.mapping.id=result_id');

PAN = LOAD '/atlas/analytics/ATLASatHOME/production' USING AvroStorage();
DESCRIBE PAN;

-- L = LIMIT PAN 10; dump L;

-- REC  = FOREACH L GENERATE result_create_time, cpu_time, elapsed_time, granted_credit, app_version_id, appid, sent_time, received_time, mod_time, opaque, id, create_time, userid, rpc_seqno, rpc_time, total_credit, expavg_credit, expavg_time, timezone, domain_name, serialnum, last_ip_addr, nsame_ip_addr, on_frac, connected_frac, active_frac, cpu_efficiency, duration_correction_factor, p_ncpus, p_vendor, p_model, p_fpops, p_iops, p_membw, os_name, os_version, m_nbytes, m_cache, m_swap, d_total, d_free, d_boinc_used_total, d_boinc_used_project, d_boinc_max, n_bwup, n_bwdown, credit_per_cpu_sec, venue, nresults_today, avg_turnaround, host_cpid, external_ip_addr, max_results_day, error_rate, product_name, gpu_active_frac;


STORE PAN  INTO 'boinc/data' USING EsStorage();
