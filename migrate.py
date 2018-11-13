#!/usr/bin/env python3
from pymongo import MongoClient
import gridfs
from pprint import pprint
from mimetypes import MimeTypes
import sys
import getopt
import csv
import argparse
import boto3
s3 = boto3.client('s3')

class Migrator():
    def __init__(self, directory, db="rocketchat", host="localhost", port=27017, bucket="mybucket", username="root", password="root"):
        self.outDir = directory
        self.log = list()
        self.db = db
        self.host = host
        self.port = port
        self.bucket = bucket
        self.username = username
        self.password = password


    def dumpfiles(self, collection):
        mime = MimeTypes()

        db = MongoClient(host=self.host, port=self.port, username=self.username, password=self.password)[self.db]
        uploadsCollection = db[collection]
        fs = gridfs.GridFSBucket(db, bucket_name=collection)

        uploads = uploadsCollection.find({}, no_cursor_timeout=True)
        instanceId = db["rocketchat_settings"].find_one({"_id": "uniqueID"})["value"]

        for upload in uploads:
            if upload["store"] == "GridFS:Uploads":
                if "complete" in upload and upload["complete"] is True:
                    path = upload["path"]
                    pathSegments = path.split("/")
                    gridfsId = pathSegments[3]
                    for res in fs.find({"_id": gridfsId}):
                        fileext = ""
                        if "extension" in upload:
                            fileext = upload["extension"]
                        else:
                            fileext = mime.guess_extension(res.content_type)
                        if fileext is not None and fileext != "":
                            filename = gridfsId+"."+fileext
                        else:
                            filename = gridfsId
                        print(upload["path"])
                        userVisitorId = None
                        if "userId" in upload:
                            objKey = instanceId+"/uploads/"+upload["rid"]+"/"+upload["userId"]+"/"+gridfsId
                            userVisitorId = upload["userId"]
                        else:
                            objKey = instanceId+"/uploads/"+upload["rid"]+"/"+upload["visitorToken"]+"/"+gridfsId
                            userVisitorId = upload["visitorToken"]
                        try:
                            objHead = s3.head_object(Bucket=self.bucket, Key=objKey)
                            print("file already exists "+upload["name"])
                        except:
                            print("uploading "+upload["name"] + "  "+ str(round(upload["size"]/1024/1024, 2)) + "  MB")
                            uploadType = ""
                            if "type" in upload:
                                uploadType = upload["type"]
                            s3.upload_fileobj(res, self.bucket, objKey, ExtraArgs={"ContentDisposition":"inline; filename=\""+upload["name"]+"\"", "ContentType": uploadType})
                        self.addtolog(gridfsId, filename, collection, res.md5, upload["rid"], userVisitorId, upload["name"])
                else:
                    print(upload)
        self.writelog()
    def addtolog(self, dbId, filename, collection, md5, rid, userId, name):
        entry = dict()
        entry["file"] = filename
        entry["id"] = dbId
        entry["collection"] = collection
        entry["md5"] = md5
        entry["rid"] = rid
        entry["userId"] = userId
        entry['name'] = name
        self.log.append(entry)

    def writelog(self):
        file = open(self.outDir+"/log.csv", "a")
        for entry in self.log:
            line = entry["id"] + "," + entry["file"] + "," + entry["collection"] + ",log" + entry["md5"] + "," + entry["rid"] + "," + entry["userId"] + "," + entry["name"] + "\n"
            file.write(line)
        file.close()

    def dedup(self):
        pass

    def updateDb(self):
        with open(self.outDir+"/log.csv") as csvfile:
            db = MongoClient(host=self.host, port=self.port, username=self.username, password=self.password)[self.db]
            reader = csv.reader(csvfile, delimiter=',')
            instanceId = db["rocketchat_settings"].find_one({"_id": "uniqueID"})["value"]
            for row in reader:
                dbId = row[0]
                filename = row[1]
                collectionName = row[2]
                md5 = row[3]
                rid = row[4]
                userId = row[5]
                name = row[6]
                collection = db[collectionName]
                collection.update_one({
                    "_id": dbId
                }, {
                    "$set": {
                        "store": "AmazonS3:Uploads",
                        "path": "/ufs/AmazonS3:Uploads/"+dbId+"/"+name,
                        "url": "/ufs/AmazonS3:Uploads/"+dbId+"/"+name,
                        "AmazonS3": {
                            "path": instanceId+"/uploads/"+rid+"/"+userId+"/"+dbId
                        }
                    }
                })
                print("updated "+name)

    def removeBlobs(self):
        with open(self.outDir + "/log.csv") as csvfile:
            db = MongoClient(host=self.host, port=self.port, username=self.username, password=self.password)[self.db]
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                dbId = row[0]
                collectionName = row[2]
                name = row[6]
                fs = gridfs.GridFSBucket(db, bucket_name=collectionName)
                try:
                    fs.delete(dbId)
                    print('deleted '+name)
                except:
                    continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-s', '--host', help='mongodb host')
    parser.add_argument('-p', '--port', help='mongodb port')
    parser.add_argument('-r', '--database', help='database')
    parser.add_argument('-c', '--command', help='[dump|updatedb|removeblobs]')
    parser.add_argument('-d', '--dir', help='files dir')
    parser.add_argument('-b', '--bucket', help='s3 bucket')
    parser.add_argument('-u', '--user', help='mongodb user')
    parser.add_argument('-x', '--password', help='mongodb password')


    parser.set_defaults(host="localhost", port=27017, database="rocketchat")

    args = parser.parse_args()

    obj = Migrator(args.dir, args.database, args.host, int(args.port), args.bucket, args.user, args.password)

    if args.command == "dump":
        obj.dumpfiles("rocketchat_uploads")

    if args.command == "updatedb":
        obj.updateDb()

    if args.command == "removeblobs":
        obj.removeBlobs()
