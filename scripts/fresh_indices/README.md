# Full re-index scripts

The "newest" op_data*.json file in exec_info is what will be used for catch-up operations. Working with any other file would require rm or touch file system commands.

The exec-info/op_data*.json file contains a dictionary keyed by strings which convert to integers indicating the order of each operation executed.

The most recent operation will have the maximum key value, and hopefully will continue to appear at the bottom.

The results of commands which "do nothing" are written to the file as well i.e. when the 'catch-up' command is executed, but no entities are determined to need re-indexing.

Because of initial use of filenames, implicity assumption that one 'create' command is executed per day.  Otherwise, need to clean up files & indices before trying another 'create' on the same day.

Each op_data*.json file in exec_info should have the operation data for a 'create' command at entry '0'. Currently, all other entries will be for subsequent 'catch-up' operations.

'catch-up' command operations re-index entries that changed in indices found in the 'create' command operation data at entry '0' into the "destination" entries in the same data.  That is ['0']['index_info'].keys() are the source indices where we will look for documents which were touched, and ['0']['index_info'][<destination index>] is where we will re-index to.
N.B. NEED TO ADD A FEATURE TO CATCH UP THE "source" INDICES RATHER THAN THE "destination" INDICES IF ALREADY SWAPPED. BUT THAT WILL MAKE THIS "OPERATION DATA" MORE COMPLEX, I THINK?



  # @TODO-KBKBKB flip to using "file_time_prefix" from op_data instead
### Steps for a full reindex KBKBKB-@TODO

1. sudo /bin/su - hive
2. pushd /opt/hubmap/cron-index-s3-json/
3. sudo cp /home/kburke/s3_to_es/* .
4. chown hive:hive *
5. docker volume inspect index_logged_info-exec_info
6. docker build -t cron-index-s3-json:initial-indexing-test .
7. docker run -it --mount "source=index_logged_info-exec_info,target=/usr/src/app/exec_info" cron-index-s3-json:initial-indexing-test 
8. ls -la /var/lib/docker/volumes/index_logged_info-exec_info/_data
