# Full re-index scripts

### Steps for a full reindex KBKBKB-@TODO

1. sudo /bin/su - hive
2. pushd /opt/hubmap/cron-index-s3-json/
3. sudo cp /home/kburke/s3_to_es/* .
4. chown hive:hive *
5. docker volume inspect index_logged_info-exec_info
6. docker build -t cron-index-s3-json:initial-indexing-test .
7. docker run -it --mount "source=index_logged_info-exec_info,target=/usr/src/app/exec_info" cron-index-s3-json:initial-indexing-test 
8. ls -la /var/lib/docker/volumes/index_logged_info-exec_info/_data
