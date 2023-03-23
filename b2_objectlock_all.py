#!/usr/bin/env python3
##
## set object locks on all files of a B2 bucket
##

import json
import subprocess
from datetime import datetime, timedelta, timezone
import time

## object lock mode & time
lockmode="governance"
lockdays=7

## cache the b2 ls into a file to avoid repeated expensive API calls
## b2 ls --json --recursive file | tee file.ls.json
filename='somefile.ls.json'

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
t_new=datetime.now(timezone.utc) + timedelta(days=lockdays)
ts_new=time.mktime(t_new.timetuple())
ts_now=time.mktime(datetime.now(timezone.utc).timetuple()) ## WTH python
print("now=%d new=%d"%(ts_now,ts_new))

## TODO: automatically regenerate ls cache file if missing or not fresh enough
## run b2 ls --json --recursive file | tee file.ls.json
##
## Do not use --versions because we want to skip the soft-deleted files
## and let the bucket Lifecycle Settings policy take care of weeding them out after the retention
## see https://www.backblaze.com/b2/docs/lifecycle_rules.html section "Object Lock"
## use Lifecycle Settings  "Keep prior versions for this number of days: N" with N=lockdays+1
##
## I also wonder if there's a risk of the "hidden" version to be deleted alone, and the old version would reappear?
## It's perfectly doable from the B2 web UI, delete the "hidden" version and the old version comes back, and this script will re-lock it
## Let's see if the Lifecycle Settings N+1 works fine on its own

## read json from ls cache file
f = open(filename)
data=json.load(f)

# Iterating through the json
for i in data:
    #print(i) ## DEBUG
    if (i["fileRetention"]["retainUntilTimestamp"]!=None) :
        ts_ret=int(i["fileRetention"]["retainUntilTimestamp"])/1000
    else:
        ts_ret=ts_now
    print("- %+06.2fd %s %s"%((ts_ret-ts_now)/(24*3600),i["fileRetention"]["mode"],i["fileName"]) )

    if ( i["action"]!='upload' ) :
        continue
    if ( i["fileRetention"]["mode"]=='unknown' ) :
        print("Error: fileRetention.mode is unknown, need capable API token")
        break
    if ( ts_ret < ts_now ):
        print("  ALERT: this object lock was EXPIRED!")

    ## set the object lock if missing or expiring in less than ts_new minus one day (e.g. 6 days if ts_new is +7 days)
    ## ok on B2 where b2_update_file_retention is class A, free
    if ( i["fileRetention"]["mode"]==None or i["fileRetention"]["retainUntilTimestamp"]==None or ts_ret+(24*60*60) < ts_new ) :
        fileId=i["fileId"]
        cmd="b2 update-file-retention --retainUntil %d %s %s "%(int(ts_new),fileId,lockmode)
        print(cmd)
        process = subprocess.run(['b2', 'update-file-retention', '--retainUntil=%d'%(int(ts_new)),fileId,lockmode ], 
                         stdout=subprocess.PIPE, 
                         universal_newlines=True)
        print(process.stdout,end='')
# Closing file
f.close()
