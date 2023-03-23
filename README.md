# objectlock
Apply object locks to all objects in a B2 bucket, and refresh their retention time.

This allows to use the [ransomware protection features of the B2 Object Locks](https://www.backblaze.com/blog/object-lock-101-protecting-data-from-ransomware/)
even without support from the backup tool. 
So you can use rclone, kopia or other tools that do not support it yet, and still enjoy the protection.

It was inspired by this feature request on kopia: https://github.com/kopia/kopia/issues/1067

## first version
This initial implementation uses the [B2 Command Line Tool](https://github.com/Backblaze/B2_Command_Line_Tool)
and needs you to hardcode all the parameters.

It should be possible to do pretty much the same thing with AWS S3 using [its CLI](https://docs.aws.amazon.com/cli/latest/reference/s3api/put-object-retention.html)

# Usage
* install [B2 Command Line Tool](https://github.com/Backblaze/B2_Command_Line_Tool)
* create an AppKey with `writeFileRetentions,readFileRetentions` permissions - the AppKeys created from the WebUI do not have those
  `b2 create-key --bucket <bucket_name> <key_name> deleteFiles,listAllBucketNames,listBuckets,listFiles,readBucketEncryption,readBucketReplications,readBuckets,readFiles,shareFiles,writeBucketEncryption,writeBucketReplications,writeFiles,writeFileRetentions,readFileRetentions`
* authenticate using 
  `b2 authorize-account <keyID>`
* generate a JSON file list 
  `b2 ls --json --recursive <bucket> | tee <bucket>.ls.json`
* adjust the code for retention settings 
  use `governance` for testing!
* run `./b2_objectlock_all.py`
* adjust the B2 bucket 
  * Lifecycle Settings to
    "Keep prior versions for this number of days: N" with N=lockdays+1
  * For testing, I think it's better not to set a default object lock mode and retention. 
    For production it may be appropriate to set a default retention larger than the interval between runs of `b2_objectlock_all`

## usage with rclone
Currently I'm testing this code with a small size backup made with rclone. 

Make sure your rclone remote config has `hard_delete = false` (use `rclone config show`).
That will make rclone use "hide" on the object, and lets the Lifecycle Settings take care of actual deletion.

With the default `hard_delete = true` rclone will try to really delete the file, and fail, and `b2_objectlock_all` will continue to update its retention.

## usage with kopia
I have not yet started testing with Kopia. It may need excluding some files like logs?

# Testing
## Testing B2, step 1
In the first round of tests, I'm not trying to bypass governance mode.
I expect this will match what I would see if I was using compliance mode already.

### delete files from rclone sync
`rclone sync` will legitimately delete files which are no longer in the source. Needs `hard_delete = false` for it to work with Object Lock.
This means that it needs to leave the actual deletion to B2 bucket Lifecycle Settings.

### delete file manually via WebUI
The "hidden" file version left by rclone deletion does not get a lock set by `b2_objectlock_all`.
This means that the "hidden" version can be deleted, and the non-hidden version will reappear, its lock may get refreshed too.
Anyway `rclone sync` should soft-delete it again at the next run. 

At worst, this will cause some extra files to reappear in a restore, if it happens before a new sync; in general it should not cause data loss.

For the files / versions which are locked, the B2 WebUI will skip them, nothing is deleted and no data is lost.

### delete files with rclone purge
tbd

may not lose data but could duplicate each object again, if a sync is done without first resurrecting the old versions?
This could result in a pretty high storage cost for the time of retention.

### delete files using full privilege 
Eg with the Master Application Key
