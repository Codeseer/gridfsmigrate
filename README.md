## RocketChat GridFS to S3 migration script

migrate -c [command] -r [dbname] -u [username] -x [password] -b [s3_bucket]

e.g. ./migrate -c dump -b my_bucket -r meteor

### commands
- dump :        dumps the GridFs stored files into the s3 bucket and writes a log(log.csv)
- updatedb :    changes the database entries to point to your stored files instead of GridFS using log.csv
- removeblobs : removes migrated files from GridFS using log.csv

### steps

1. for safety do a mongo backup with mongodump
2. switch RocketChat to S3 and set bucket to my_bucket or similar
3. run ./migrate -c dump -b my_bucket -r rocketchat
4. run ./migrate -c updatedb -b my_bucket -r rocketchat
5. have a look, if everything looks fine e.g are files missing etc.
6. run ./migrate -c removeblobs -b my_bucket -r rocketchat

