#!/usr/bin/env python3
## start testing with Boto3
## config file only half works
## no support for paged responses yet
## whole thing needs refactoring to avoid globals
## https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client

import boto3
#import json
from pprint import pprint
import logging
import os
## avoid warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def list_folder(s3, bucket_name):
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
    print("Listing bucket " + bucket_name + ":")
    #objects = s3.list_objects(Bucket=bucket_name)
    objects = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in objects:
        for key in objects['Contents']:
            print(key['Key'])
            print(key)
    else:
        print("Is empty")


def set_objectlock(s3,bucket_name,key,version,date):
    try:
        vret=s3.put_object_retention(Bucket=bucket_name,Key=key,VersionId=version,Retention={'Mode': 'GOVERNANCE','RetainUntilDate':date})
        del vret['ResponseMetadata']
    except BaseException as exception:
        #print( "  No ObjectLock for %s v%s"%(key,version) )
        logging.warning(f"Exception Name: {type(exception).__name__}")
        logging.warning(f"Exception Desc: {exception}")
    return

def getobjectlock(s3,bucket_name,key,version):
    try:
        vret=s3.get_object_retention(Bucket=bucket_name,Key=key,VersionId=version)
        del vret['ResponseMetadata']
    except BaseException as exception:
        #print( "  No ObjectLock for %s v%s"%(key,version) )
        #logging.warning(f"Exception Name: {type(exception).__name__}")
        #logging.warning(f"Exception Desc: {exception}")
        return {'Mode': None, 'RetainUntilDate': None }
    return vret["Retention"]

def objectlock(s3,bucket_name):
    print("##########################################################"*4)
    ## https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_object_versions.html
    ## counters
    ver_nolock_n=0
    ver_nolock_s=0
    ver_keep_n=0
    ver_keep_s=0
    ver_exp_n=0
    ver_exp_s=0
    last_n=0
    last_s=0
    total_n=0
    total_s=0
    #obj=s3.list_object_versions(Bucket=bucket,Prefix=file)
    obj=s3.list_object_versions(Bucket=bucket_name,Prefix="",MaxKeys=2000)
    del obj['ResponseMetadata']
    #pprint(obj,width=200)

    ## index the deleted objects
    obj_deleted={}
    if ( "DeleteMarkers" in obj ):
        for v in obj["DeleteMarkers"]:
            key=v["Key"]
            del v["Owner"]
            if ( key in obj_deleted ):
                ## for now let's assume this does not happen, just catch it if it does
                print("## ALERT: multiple DeleteMarkers for %s"%(key))
                print(v)
                print(obj_deleted[key])
                exit(1)
            obj_deleted[key]=v

    for v in obj["Versions"]:
        del v["Owner"]
        del v["ETag"] # what is ETag anyway? it's not unique
        #print(v["Key"],": ",v)
        total_n+=1
        total_s+=v["Size"]

        if ( v["IsLatest"] == False ): ## safer as it will not match unknown/null
            continue
        last_n+=1
        last_s+=v["Size"]
        ol=getobjectlock(s3,bucket_name,v["Key"],v["VersionId"])
        if ( ol["Mode"]==None ):
            ver_nolock_n+=1
            ver_nolock_s+=v["Size"]
            dt=0
        else:
            dt=(ol["RetainUntilDate"]-today).total_seconds()/(24*60*60) ## days
            #ver_keep_n+=1
            #ver_keep_s+=v["Size"]

        update=False
        state="-"
        if ( dt<0 ):
            #print("  EXPIRED ",ol["RetainUntilDate"] )
            state="E"
            update=True
        if ( dt>0 ):
            #print("  Retention OK",ol["RetainUntilDate"])
            state="-"
        print("%s %+06.2fd %s %s"%(state,dt,ol["Mode"],v["Key"]) )
        if ( args.update and update ):
            date_new=today+timedelta(days=args.lockdays)
            print( "  updating retention of %s to %s"%(v["Key"],date_new) )
            set_objectlock(s3,bucket_name,v["Key"],v["VersionId"],date_new)

        continue
    # end for obj

    ## how much overhead due to object locking ?
    #overhead=(ver_keep_s+ver_exp_s)/last_s*100
    overhead=100*(total_s/last_s)-100
    print( "Summary: last %d %fMiB nolock %d %fMiB keep %d %fMiB expired %d %fMiB overhead %.2f%% "%( last_n,last_s/2**20, ver_nolock_n,ver_nolock_s/2**20, ver_keep_n,ver_keep_s/2**20, ver_exp_n,ver_exp_s/2**20, overhead ) )
    print( "Check total: n %d/%d s %d/%d"%( last_n+ver_nolock_n+ver_keep_n+ver_exp_n,total_n, last_s+ver_nolock_s+ver_keep_s+ver_exp_s,total_s) )
    print( "List IsTruncated: %s"%(obj["IsTruncated"]) )

# end def objectlock

#### def main please


## some globals
from datetime import datetime,timezone,timedelta
today = datetime.now(timezone.utc)
args = None

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="check or update B2 Object Locks"
    )
    ## https://stackoverflow.com/questions/25295487/python-argparse-value-range-help-message-appearance
    #parser.add_argument("--profile",  help="B2 profile", required=True)
    parser.add_argument("--fileagemax", help="max age of ls cache file in seconds, default=600", default=(10*60))
    parser.add_argument("--lockmode", help="Lock mode",choices=["governance"],default="governance") ## play safe
    #parser.add_argument("--lockmode", help="Lock mode",choices=["governance","compliance"],default="governance")
    parser.add_argument("--lockdays", help="Lock for days, default=7",type=int,default=7,choices=range(0, 91),metavar="[0-90]" )
    parser.add_argument("--lockmax",  help="Max Lock for days, default=30",type=int,default=30,choices=range(0,91),metavar="[0-90]" )
    parser.add_argument("--update",   help="Update lock mode & days",action="store_true")
    parser.add_argument("--iskopia",  help="Ignore Kopia logs & maintenance",action="store_true")
    parser.add_argument("bucket")
    parser.add_argument("path",default="",nargs='?')
    global args
    args = parser.parse_args()
    ## some argument sanity checks
    if ( args.lockmax <= args.lockdays ):
        args.lockmax = (args.lockdays * 1.10)
        print("WARNING: lockmax < lockdays, reset to %f days"%args.lockmax)
    ## debug args
    print(args)

    # aws config is crap
    # https://stackoverflow.com/questions/32618216/override-s3-endpoint-using-boto3-configuration-file
    # Let's not use Amazon S3 but local minio
    url="https://minio.home:9000"
    #os.environ["AWS_ENDPOINT_URL"]=url
    #boto3.set_stream_logger('', logging.DEBUG)
    boto3.setup_default_session(profile_name='minio')

    s3 = boto3.client('s3',endpoint_url=url,verify=False)
    #s3 = boto3.client('s3',verify=False)
    #print(s3)
    #print(s3.list_buckets())

    buckets = s3.list_buckets()
    #print(buckets)
    pprint(buckets,indent=4,depth=10,width=8)

    #s3 = boto3.resource('s3')
    # Print out bucket names
    for bucket in buckets["Buckets"]:
        print(bucket["Name"])
        #list_folder(s3,bucket["Name"])
    print("##########################################################"*4)

    print(s3.get_object_lock_configuration(Bucket=args.bucket)["ObjectLockConfiguration"])
    print("##########################################################"*4)
    objectlock(s3,args.bucket)

if ( __name__ == "__main__" ):
    main()
    exit(0)

while True:
    if ( v["Key"] in obj_deleted ):
        print("  Deleted with ",obj_deleted[v["Key"]])
        #ver_del_n+=1
        #ver_del_s+=v["Size"]
        #continue
    print(vret)
    ver_exp_n+=1
    ver_exp_s+=v["Size"]
    continue
    ## cleanup old versions
    try:
        res=s3.delete_object(Bucket=bucket,Key=v["Key"],VersionId=v["VersionId"])
        print(res)
    except BaseException as exception:
        logging.warning(f"Exception Name: {type(exception).__name__}")
        logging.warning(f"Exception Desc: {exception}")
        print(res)

