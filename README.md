# objectlock
Apply object locks to all objects in a B2 bucket

This allows to use the [ransomware protection features of the B2 Object Locks](https://www.backblaze.com/blog/object-lock-101-protecting-data-from-ransomware/)
even without support from the backup tool. 
So you can use rclone, kopia or other tools that do not support it yet, and still enjoy the protection.

It was inspired by this feature request on kopia: https://github.com/kopia/kopia/issues/1067

## first version
This initial implementation uses the [B2 Command Line Tool](https://github.com/Backblaze/B2_Command_Line_Tool)
and needs you to hardcode all the parameters.

It should be possible to do pretty much the same thing with AWS S3 using [its CLI](https://docs.aws.amazon.com/cli/latest/reference/s3api/put-object-retention.html)
