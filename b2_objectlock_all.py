#!/usr/bin/env python3
##
## set object locks on all files of a B2 bucket
##

import json
import subprocess
from datetime import datetime, timedelta, timezone
import time

import argparse
parser = argparse.ArgumentParser(
    description="check or update B2 Object Locks"
)
## https://stackoverflow.com/questions/25295487/python-argparse-value-range-help-message-appearance
parser.add_argument("--profile",  help="B2 profile", required=True)
parser.add_argument("--fileagemax", help="max age of ls cache file in seconds, default=600", default=(10*60))
parser.add_argument("--lockmode", help="Lock mode",choices=["governance"],default="governance") ## play safe
#parser.add_argument("--lockmode", help="Lock mode",choices=["governance","compliance"],default="governance")
parser.add_argument("--lockdays", help="Lock for days, default=7",type=int,default=7,choices=range(0, 91),metavar="[0-90]" )
parser.add_argument("--lockmax",  help="Max Lock for days, default=30",type=int,default=30,choices=range(0,91),metavar="[0-90]" )
parser.add_argument("--update",   help="Update lock mode & days",action="store_true")
parser.add_argument("--iskopia",  help="Ignore Kopia logs & maintenance",action="store_true")
parser.add_argument("bucket")
parser.add_argument("path",default="")
args = parser.parse_args()
## some argument sanity checks
if ( args.lockmax <= args.lockdays ):
    args.lockmax = (args.lockdays * 1.10)
    print("WARNING: lockmax < lockdays, reset to %f days"%args.lockmax)
## debug args
print(args)

## object lock mode & time
#lockmode="governance"
#lockdays=7

## cache the b2 ls into a file to avoid repeated expensive API calls
## b2 ls --json --recursive file | tee file.ls.json
filename="objectlock.%s.ls.json"%(args.bucket)

## ensure that we don't set locks to a far future because of a bad local clock
## verify local time with curl "http://worldtimeapi.org/api/timezone/Etc/UTC"
## also https://timeapi.io/swagger/index.html works, but it's missing unix time
import requests
def check_local_clock():
    #res=requests.get("http://worldtimeapi.org/api/timezone/Etc/UTC") ## very rate limited? fails most of the time
    res=requests.get("https://timeapi.io/api/Time/current/zone?timeZone=Etc/UTC")
    res.raise_for_status()
    data=json.loads(res.content.decode())
    date=datetime(data["year"], data["month"], data["day"], data["hour"], data["minute"], data["seconds"], tzinfo=timezone.utc)
    #date=datetime(data["year"], data["month"], data["day"], tzinfo=timezone.utc) ## DEBUG use a bad parsing to test mismatch
    dt=abs(date-datetime.now(timezone.utc))
    if ( dt > timedelta(minutes=10) ) :
        print("FATAL: offset to timeapi.io is %s, larger than 10 minutes, aborting"%dt)
        print("  timeapi.io = %s local clock UTC = %s"%(date,datetime.now(timezone.utc)) )
        exit(1)

check_local_clock()

## unix epoch for new objectlock timestamp
t_new=datetime.now(timezone.utc) + timedelta(days=args.lockdays)
ts_new=time.mktime(t_new.timetuple())
ts_now=time.mktime(datetime.now(timezone.utc).timetuple()) ## WTH python
print("now=%d new=%d"%(ts_now,ts_new))

## automatically regenerate ls cache file if missing or not fresh enough
## run b2 ls --json --recursive bucket path | tee file.ls.json
##
## Do not use --versions because we want to skip the soft-deleted files
## and let the bucket Lifecycle Settings policy take care of weeding them out after the retention
## see https://www.backblaze.com/b2/docs/lifecycle_rules.html section "Object Lock"
## use Lifecycle Settings  "Keep prior versions for this number of days: N" with N=lockdays+1
##
## I also wonder if there's a risk of the "hidden" version to be deleted alone, and the old version would reappear?
## It's perfectly doable from the B2 web UI, delete the "hidden" version and the old version comes back, and this script will re-lock it
## Let's see if the Lifecycle Settings N+1 works fine on its own

def file_age(filename):
    import time, os
    try:
        return time.time()-os.path.getmtime(filename)
    except:
        return 1e12 ## kind of impossible age

fileage=file_age(filename)
if ( fileage > args.fileagemax ):
    cmdargs=['b2', 'ls', "--profile=%s"%(args.profile),'--recursive',"--json",args.bucket,args.path]
    print(cmdargs)
    process = subprocess.run(cmdargs,
         stdout=subprocess.PIPE,
         universal_newlines=True)
    ## print(process.stdout,end='') # DEBUG
    with open(filename, "w") as text_file:
        text_file.write(process.stdout)
else:
    print("Use existing %s age %.2f minutes"%(filename,fileage/60))
#exit(0)


## read json from ls cache file
f = open(filename)
data=json.load(f)
n_files=0
n_updates=0
t_size=0
# Iterating through the json
for i in data:
    #print(i) ## DEBUG
    if (i["fileRetention"]["retainUntilTimestamp"]!=None) :
        ts_ret=int(i["fileRetention"]["retainUntilTimestamp"])/1000
    else:
        ts_ret=ts_now
    print("- %+06.2fd %s %s"%((ts_ret-ts_now)/(24*3600),i["fileRetention"]["mode"],i["fileName"]) )
    n_files+=1
    t_size+=i["size"]

    if ( i["action"]!='upload' ) :
        print("  WARN: action=%s"%(i["action"]) )
        continue
    if ( i["fileRetention"]["mode"]=='unknown' ) :
        print("Error: fileRetention.mode is unknown, need capable API token")
        break
    if ( args.iskopia ):
        if ( i["fileName"].startswith("_log_") or i["fileName"] == "kopia.maintenance" ) :
            ## ignore kopia logs and maintenance
            continue
    if ( ts_ret < ts_now ):
        print("  ALERT: this object lock was EXPIRED!")
    elif ( (ts_ret-ts_now)>(args.lockmax*24*60*60) ):
        print("  ALERT: this object lock is too far in the future, >%d days"%args.lockmax)
        print("    b2 update-file-retention --profile MAK --bypassGovernance --retainUntil=%d %s %s %s"%(int(ts_new),i["fileName"],i["fileId"],"governance") )

    ## set the object lock if missing or expiring in less than ts_new minus one day (e.g. 6 days if ts_new is +7 days)
    ## ok on B2 where b2_update_file_retention is class A, free
    if ( i["fileRetention"]["mode"]==None or i["fileRetention"]["retainUntilTimestamp"]==None or ts_ret+(24*60*60) < ts_new ) :
        n_updates+=1
        if ( args.update==False ) :
            print("  ALERT: needs update %s"%(i["fileName"]))
            continue
        fileId=i["fileId"]
        cmdargs=['b2', 'update-file-retention', "--profile=%s"%(args.profile), '--retainUntil=%d'%(int(ts_new)),fileId,args.lockmode ]
        print("  Update: %s"%(" ".join(cmdargs)))
        process = subprocess.run(cmdargs,
                         stdout=subprocess.PIPE, 
                         universal_newlines=True)
        print(process.stdout,end='')
        if ( process.returncode !=0 ) :
            print("FATAL: b2 returned an error code %d"%process.returncode)
            exit(1)
# Closing file
f.close()

print("Summary:")
print("  n_files : %d"%n_files)
print("  n_update: %d"%n_updates)
print("  t_size  : %0.2fGiB"%(t_size/(1024*1024*1024)))
