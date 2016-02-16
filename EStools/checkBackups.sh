echo "all snapshots:\n"
curl -XGET 'http://atlas-kibana.mwt2.org:9200/_snapshot'

# curl -XPUT 'http://atlas-kibana.mwt2.org:9200/_snapshot/my_backup' -d '{"type": "fs","settings": {"location": "/backup","max_snapshot_bytes_per_sec" : "50mb", "max_restore_bytes_per_sec" : "150mb" } }'

echo "\nverify ..."
curl -XPOST 'http://atlas-kibana.mwt2.org:9200/_snapshot/my_backup/_verify'
echo "\n-------------------------------------------------------------------"


echo "check the yesterday's backup\n"
Yest=$(date +%Y-%m-%d -d "-1days")
curl -XGET http://atlas-kibana.mwt2.org:9200/_snapshot/my_backup/${Yest}

