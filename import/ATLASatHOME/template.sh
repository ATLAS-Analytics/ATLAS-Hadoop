curl -XPOST 'http://atlas-kibana.mwt2.org:9200/_template/boinc' -d '
{
  "template" : "boinc*",
  "settings" : {       "number_of_shards" : 5,        "number_of_replicas" : 0    },
  "mappings": {
      "data": {
        "properties": {
	"host_name":{ "type": "keyword" } ,
	"result_name":{ "type": "keyword" } ,
	"name":{ "type": "keyword" } ,
	"result_template_file":{ "type": "keyword" } ,
	"xml_doc":{ "type": "keyword" } ,
	"result_received_time":{ "type": "date","format":"epoch_second" },
	"result_mod_time":{ "type": "date","format":"epoch_second" },
	"result_create_time":{ "type": "date","format":"epoch_second" },
	"result_sent_time":{ "type": "date","format":"epoch_second" }
	}
    }
 }
}'
