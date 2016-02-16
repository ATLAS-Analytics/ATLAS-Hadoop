echo "all snapshots:"
curl -XGET 'http://atlas-kibana.mwt2.org:9200/_snapshot'

# curl -XPUT 'http://atlas-kibana.mwt2.org:9200/_snapshot/my_backup' -d '{"type": "fs","settings": {"location": "/backup","max_snapshot_bytes_per_sec" : "50mb", "max_restore_bytes_per_sec" : "150mb" } }'

echo "verify ..."
curl -XPOST 'http://atlas-kibana.mwt2.org:9200/_snapshot/my_backup/_verify'


