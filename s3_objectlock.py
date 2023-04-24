#!/usr/bin/env python3
## start testing with Boto3
## config file only half works
## no support for paged responses yet
## whole thing needs refactoring to avoid globals
## https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client

import boto3
import botocore.exceptions
from pprint import pprint
import logging
import os
## avoid urllib warnings for ignored cert validation
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

## start refactoring
class S3Bucket:

    def __init__(self,s3,bucket_name):
        self.s3=s3
        self.bucket_name=bucket_name
        self.obj_deleted={}
        ## counters
        self.ver_nolock_n=0
        self.ver_nolock_s=0
        self.ver_exp_n=0
        self.ver_exp_s=0
        self.ver_n=0
        self.ver_s=0
        self.cur_n=0
        self.cur_s=0
        self.cur_nolock_n=0
        self.cur_nolock_s=0
        self.cur_exp_n=0
        self.cur_exp_s=0
        self.ops_cleanup_n=0
        self.ops_cleanup_s=0
        self.ops_extend_n=0
        self.ops_extend_s=0
        self.total_n=0
        self.total_s=0


    def head_bucket(self):
        print("##########################################################"*4)
        res=self.s3.head_bucket(Bucket=self.bucket_name)
        del res['ResponseMetadata']
        print(res)

    def get_object_lock_configuration(self):
        print("##########################################################"*4)
        res=self.s3.get_object_lock_configuration(Bucket=self.bucket_name)
        print(res["ObjectLockConfiguration"])

    def list_buckets(self):
        buckets = self.s3.list_buckets()
        #print(buckets)
        #pprint(buckets,indent=4,depth=10,width=8)
        print("Existing buckets:")
        # Print out bucket names
        for bucket in buckets["Buckets"]:
            print("- ",bucket["Name"])
            #list_folder(s3,bucket["Name"])

    def list_folder(self):
        ## https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
        print("Listing bucket " + self.bucket_name + ":")
        #objects = self.s3.list_objects(Bucket=self.bucket_name)
        objects = self.s3.list_objects_v2(Bucket=self.bucket_name)
        if 'Contents' in objects:
            for key in objects['Contents']:
                print(key['Key'])
        else:
            print("Is empty")

    def set_objectlock(self,key,version,date):
        try:
            vret=self.s3.put_object_retention(Bucket=self.bucket_name,Key=key,VersionId=version,
                Retention={'Mode': 'GOVERNANCE','RetainUntilDate':date})
            #del vret['ResponseMetadata']
        except KeyboardInterrupt:
            raise SystemExit
        except BaseException as exception:
            logging.warning(f"Exception Name: {type(exception).__name__}")
            logging.warning(f"Exception Desc: {exception}")
        return

    def get_objectlock(self,key,version):
        try:
            vret=self.s3.get_object_retention(Bucket=self.bucket_name,Key=key,VersionId=version)
            #del vret['ResponseMetadata']
        except KeyboardInterrupt:
            raise SystemExit
        except BaseException as exception:
            #logging.warning(f"Exception Name: {type(exception).__name__}")
            #logging.warning(f"Exception Desc: {exception}")
            return {'Mode': None, 'RetainUntilDate': None }
        return vret["Retention"]

    def objectlock(self):
        ## the response of list_object_versions is split in pages.
        ## use the next markers to iterate over pages
        NextKeyMarker=""
        NextVersionIdMarker=""
        print("##########################################################"*4)
        try:
            while True:
                NextKeyMarker,NextVersionIdMarker = self.objectlock_page(NextKeyMarker,NextVersionIdMarker)
                #print(NextKeyMarker,NextVersionIdMarker)
                if (NextKeyMarker==None):
                    break
        except (KeyboardInterrupt,SystemExit) as exception:
            #logging.warning(f"KI Exception Desc: {exception}")
            pass
        except BaseException as exception:
            logging.warning(f"BE Exception Name: {type(exception).__name__}")
            logging.warning(f"BE Exception Desc: {exception}")
            raise exception

        ## how much overhead due to object locking ?
        if ( self.cur_s>0 ):
            overhead=100*(self.total_s/self.cur_s)-100
        else:
            overhead=100
        print()
        print( "Summary %s: current %d %fMiB version %d %fMiB ver_expired %d %fMiB overhead %.2f%% "%
            ( self.bucket_name, self.cur_n,self.cur_s/2**20, self.ver_n,self.ver_s/2**20, self.ver_exp_n,self.ver_exp_s/2**20, overhead ) )
        print( "   current : %5s %7.2fMiB"%(self.cur_n,       self.cur_s/2**20))
        print( "     nolock: %5s %7.2fMiB"%(self.cur_nolock_n, self.cur_nolock_s/2**20))
        print( "     expird: %5s %7.2fMiB"%(self.cur_exp_n,    self.cur_exp_s/2**20))
        print( "   version : %5s %7.2fMiB"%(self.ver_n,        self.ver_s/2**20))
        print( "     nolock: %5s %7.2fMiB"%(self.ver_nolock_n, self.ver_nolock_s/2**20))
        print( "     expird: %5s %7.2fMiB"%(self.ver_exp_n,    self.ver_exp_s/2**20))
        print( "   cleanup : %5s %7.2fMiB"%(self.ops_cleanup_n,self.ops_cleanup_s/2**20))
        print( "   extend  : %5s %7.2fMiB"%(self.ops_extend_n, self.ops_extend_s/2**20))
        print( "Check total: n %d/%d s %d/%d"%( self.cur_n+self.ver_n,self.total_n, self.cur_s+self.ver_s,self.total_s) )

    def objectlock_page(self,NextKeyMarker,NextVersionIdMarker):
        ## https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_object_versions.html
        #obj=self.s3.list_object_versions(Bucket=bucket,Prefix=file)
        obj=self.s3.list_object_versions(Bucket=self.bucket_name,Prefix="",MaxKeys=1000,KeyMarker=NextKeyMarker,VersionIdMarker=NextVersionIdMarker)
        #del obj['ResponseMetadata']
        #print( "List IsTruncated: %s"%(obj["IsTruncated"]) )
        #pprint(obj,width=200)

        ## index the deleted objects
        #print("##########################################################"*4)
        self.obj_deleted={}
        if ( "DeleteMarkers" in obj ):
            for v in obj["DeleteMarkers"]:
                key=v["Key"]
                if ( key in self.obj_deleted ):
                    ## if there are multiple delete markers it probably does not matter
                    ## only one of them should be "isLatest"
                    if ( not args.quiet ):
                        print("## Warning: multiple DeleteMarkers for %s"%(key))
                        print(" prev",self.obj_deleted[key])
                        print(" new ",v)
                        #del v["Owner"]
                        #pprint(obj["DeleteMarkers"])
                ## ignore old delete markers
                if ( v["IsLatest"] ):
                    self.obj_deleted[key]=v

        ## iterate over object versions
        for v in obj["Versions"]:
            #del v["Owner"]
            #del v["ETag"] # what is ETag anyway? it's not unique
            #print(v["Key"],": ",v)
            self.total_n+=1
            self.total_s+=v["Size"]

            ## current, old version or deleted
            current= not ( v["IsLatest"] == False ) ## safer as it will not match unknown/null
            if ( current ): 
                self.cur_n+=1
                self.cur_s+=v["Size"]
                cstate="-"
            else:
                self.ver_n+=1
                self.ver_s+=v["Size"]
                if ( v["Key"] in self.obj_deleted ):
                    cstate="D"
                else:
                    cstate="V"

            ## get and check object lock mode and time
            ol=self.get_objectlock(v["Key"],v["VersionId"])
            if ( ol["Mode"]==None ):
                dt=0
                if current:
                    self.cur_nolock_n+=1
                    self.cur_nolock_s+=v["Size"]
                else:
                    self.ver_nolock_n+=1
                    self.ver_nolock_s+=v["Size"]
            else:
                dt=(ol["RetainUntilDate"]-today).total_seconds()/(24*60*60) ## days

            age=(today-v["LastModified"]).total_seconds()/(24*60*60) ## days

            expired=False
            update=False
            exempt=False
            state="-"
            if ( args.iskopia ): ## ignore kopia logs and maintenance
                if ( v["Key"].startswith("_log_") or v["Key"].startswith("s") or v["Key"] == "kopia.maintenance" ) :
                    exempt=True

            ## check OL mode and expiration date
            if ( ol["Mode"] == None or ol["Mode"] != args.lockmode ): 
                if ( not exempt ) :
                    state="N"
                    update=True
            elif ( dt < 0 ) :
                state="E"
                expired=True
                update=True
            elif ( dt < args.lockdays/2 ) :
                update=True
            elif ( dt > args.lockmax ):
                state="M"

            ## collect some stats
            if ( expired ) :
                if ( current ) :
                    self.cur_exp_n+=1
                    self.cur_exp_s+=v["Size"]
                else:
                    self.ver_exp_n+=1
                    self.ver_exp_s+=v["Size"]

            if ( args.quiet == False or state != "-"):
                print("%1s%1s %+ 7.2fd % 6.2fd %6.2fMiB %s %s"%(cstate,state,dt,age,v["Size"]/2**20,ol["Mode"],v["Key"]) )
            if ( state=="X" ):
                print("  ALERT: this object lock is too far in the future, >%d days"%args.lockmax)
                #print("    b2 update-file-retention --profile MAK --bypassGovernance --retainUntil=%d %s %s %s"%(int(ts_new),v["Key"],i["fileId"],"governance") )

            ## clean-up old versions and deleted files
            ## bucket lifecycle should take care of this actually, so this is mostly a sanity check
            if ( not current and dt<=0 and age > args.cleanage ):
                self.ops_cleanup_n+=1
                self.ops_cleanup_s+=v["Size"]
                if ( args.cleanup ):
                    print( "  apply clean up % 6.2fd old %s v%s"%(age,v["Key"],v["VersionId"]) )
                    self.s3.delete_object(Bucket=self.bucket_name,Key=v["Key"],VersionId=v["VersionId"])
                else:
                    print( "  should clean up % 6.2fd old %s v%s"%(age,v["Key"],v["VersionId"]) )

            ## only update retention for current version
            if ( not exempt and current and update ):
                self.ops_extend_n+=1
                self.ops_extend_s+=v["Size"]
                date_new=today+timedelta(days=args.lockdays)
                if ( args.extend ):
                    print( "  apply extend retention of %s to %s (%.2f->%.2f)"%(v["Key"],date_new,dt,args.lockdays) )
                    self.set_objectlock(v["Key"],v["VersionId"],date_new)
                else:
                    print( "  should extend retention of %s to %s (%.2f->%.2f)"%(v["Key"],date_new,dt,args.lockdays) )
        # end for obj

        ## next page?
        if ( obj["IsTruncated"] ):
            return obj["NextKeyMarker"],obj["NextVersionIdMarker"]
        else:
            return None, None

    # end def objectlock

#### def main please


## some globals
from datetime import datetime,timezone,timedelta
today = datetime.now(timezone.utc)
args = None

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="check or update S3/B2 Object Locks"
    )
    ## https://stackoverflow.com/questions/25295487/python-argparse-value-range-help-message-appearance
    parser.add_argument("--lockmode", help="Lock mode",choices=["governance"],default="governance") ## play safe
    #parser.add_argument("--lockmode", help="Lock mode",choices=["governance","compliance"],default="governance")
    parser.add_argument("--lockdays", help="Lock for days, default=7",type=int,default=7,choices=range(0, 91),metavar="[0-90]" )
    parser.add_argument("--lockmax",  help="Max Lock for days, default=30",type=int,default=30,choices=range(0,91),metavar="[0-90]" ) ## to be re-implemented
    parser.add_argument("--cleanage", help="Age threshold for clean-up versions and deleted files",type=int,default=7,choices=range(0,91),metavar="[0-90]" )
    parser.add_argument("--cleanup",  help="Apply cleanup",action="store_true")
    parser.add_argument("--extend",   help="Set lock mode & extend retention days",action="store_true")
    parser.add_argument("--iskopia",  help="Ignore Kopia logs & maintenance files",action="store_true")
    parser.add_argument("--quiet",    help="Low verbosity",action="store_true")
    parser.add_argument("bucket")
    parser.add_argument("path",default="",nargs='?')
    global args
    args = parser.parse_args()
    ## some argument sanity checks
    if ( args.lockmax <= args.lockdays ):
        args.lockmax = (args.lockdays * 1.10)
        print("WARNING: lockmax < lockdays, reset to %f days"%args.lockmax)
    ## parser.print_help(sys.stderr)
    args.lockmode=args.lockmode.upper()
    args.profile=args.bucket
    ## debug args
    print(args)

    ## setup logging
    #boto3.set_stream_logger('', logging.DEBUG)

    ## setup session and get config info
    ## aws config is crap
    ## https://stackoverflow.com/questions/32618216/override-s3-endpoint-using-boto3-configuration-file
    ## so I get the full config info and feed custom params back into boto
    ## https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    ## https://github.com/boto/boto3/blob/develop/boto3/session.py
    ## https://github.com/boto/botocore/blob/develop/botocore/session.py
    try:
        boto3.setup_default_session(profile_name=args.profile)
    except botocore.exceptions.ProfileNotFound as exception:
        logging.warning(f"Exception: {type(exception).__name__}: {exception}")
        try:
            (args.profile,args.bucket)=args.profile.split('.',1)
            boto3.setup_default_session(profile_name=args.profile)
        except botocore.exceptions.ProfileNotFound as exception:
            logging.warning(f"Exception: {type(exception).__name__}: {exception}")
            print(" No profile found matching %s or %s.%s. Check your ~/.aws/config file."%(args.profile,args.profile,args.bucket) )
            return 1
    s=boto3._get_default_session()
    cfg=s._session.get_scoped_config()
    #pprint(s._session.get_scoped_config())
    cfg_url=cfg["endpoint_url"]
    try:
        cfg_verify=True
        cfg_verify=cfg["https_validate_certificates"]
        if ( cfg_verify=="False" or cfg_verify=="false" ):
            cfg_verify=False
    except:
        pass
    if ( "bucket" in cfg ):
        args.bucket = cfg["bucket"]

    s3 = boto3.client('s3',endpoint_url=cfg_url,verify=cfg_verify)
    #s3 = boto3.client('s3',verify=False)
    #print(s3)
    #print(s3.list_buckets())

    bucket=S3Bucket(s3,args.bucket)
    try:
        bucket.head_bucket()
    except botocore.exceptions.ClientError as exception:
        logging.warning(f"Exception Desc: {type(exception).__name__}: {exception}")
        bucket.list_buckets()
        return 1

    try:
        bucket.get_object_lock_configuration()
    except botocore.exceptions.ClientError as exception:
        logging.warning(f"Exception Desc: {type(exception).__name__}: {exception}")
        pass

    bucket.objectlock()
    return 0


#####################################################################

if ( __name__ == "__main__" ):
    import sys
    sys.exit(main())
