# Storage Transition to Glacier
## Dependencies
It requires yig-iam fix in https://github.com/wuxingyi/yig-iam/pull/1    

## Modules
![arch](https://github.com/woclouds3/yig/blob/glacier/doc/picture/glacier_modules.png)  

## Configuration and Constants in Implementation
Quick Facts in https://docs.aws.amazon.com/amazonglacier/latest/dev/uploading-archive-mpu.html  
* yig.toml
```
enable_glacier = true
glacier_host = "10.0.0.1:9100"
glacier_region = "us-east-1"
glacier_tier = "Standard"
hidden_bucket_lc_thread = 5 
```
* storage/glacier.go and storage/glacier-multipart.go
```
DEFAULT_VAULT_NAME  = "-"    
DEFAULT_PART_SIZE               = (512 * MB)  
MAX_GLACIER_PART_NUM_PER_UPLOAD = 10000  
```
* tools/lc.go  
```
DEFAULT_LC_SLEEP_INTERVAL_HOUR     = 1
DEFAULT_LC_WAIT_FOR_GLACIER_MINUTE = 30
DEFAULT_LC_QUEUE_LENGTH            = 100
```

## Work Flow
* Vault creation  
Vault is created per bucket when yig receives a lifecycle configuration with transition for the first time.  
Vault won't be deleted as there is no good time point. And as long as a Vault exist, user can find the disks (from ehualu).  
Please find the implementation in storage/glacier.go [CreateVault()].   
  
* Storage Transition (from Ceph to Glacier)
LC daemon will periodically check lifecycle configuration and delete expired objects.  
Now it also transit expired objects to Glacier.  
It's designed that, if a bucket is configured with transition to Glacier and yig is configured with Glacier enabled, **objects in it are not appendable**, to avoid possible data loss in simutanous operation to the same object.
Small file will be transitted as a single archive, and multipart big file will be transitted in equal size part (512MB).   
Please find the implementation in storage/glacier.go [TransitObjectToGlacier()].   
![arch](https://github.com/woclouds3/yig/blob/glacier/doc/picture/glacier_transit.png)

* Multipart to Glacier   
Objects transited to Glacier will be wrapped in multipart (512MB long per part) if it's saved in yig in multipart.   
If len(object.Parts) is of 0, the object will be transited to Glacier without multipart.  
Multipart to Glacier is of fixed length, while multipart in S3 can be of any size. So in storage/glacier-multipart.go, it's implemented that S3 object parts are cut into 512MB parts.  
Please find the limitations later in this doc.
Please find the implementation in storage/glacier-multipart.go which is called by storage/glacier.go [TransitObjectToGlacier()].   

* Restore & Get  
After an object archived, the user must restore it before get it which may take some time.  
The user can indicate the restored days in the request.  
LC will delete the restored copy after that.  
**In our design, a "hidden" bucket will be created for each user to save the temperary restored copy. Glacier backend (ehualu) will PUT the object to the hidden bucket as indicated by yig in Job initiation request. It'll take some time.**   
Please find the restore implementation in api/object-handler.go [RestoreObjectHandler()] and storage/glacier.go [RestoreObjectFromGlacier()].   
After restore, the restored object copy in hidden bucket will be returned when GET is requested.  
Please find the GET implementation in api/object-handler.go [GetObjectHandler()].  
![arch](https://github.com/woclouds3/yig/blob/glacier/doc/picture/glacier_restore_and_get.png)

* Delete   
There is no different between deleting an STANDARD object and a Glacier object.  
The only implementation difference is in delete daemon which calls different API for different storage class.  
Please find the implementation in storage/glacier.go [DeleteObjectFromGlacier()].   
![arch](https://github.com/woclouds3/yig/blob/glacier/doc/picture/glacier_delete.png)  

## Limitation
* **There may be problems when multiple lc daemons do transition at the same time.**  
There is no problem if multiple lc daemons delete expired objects simutanously.
But when multiple lc daemons transit objects to Glacier without any synchronization mechanism, the same object may be saved in Glacier for more than once (unwanted).
Further work is required on multiple lc transition.
* AppendObject is not supported on buckets configured with Transition rules, to keep data integrity.  
* SSE objects will not be transited to Glacier.  
* Vaults are not deleted.  
* Current Glacier backend (ehualu) support smallest single part 8M, while AWS Glacier defines smallest part 1M.
* Current Glacier backend (ehualu) support max archive 4T, while AWS S3 max object 5T.  
* Current Glacier multipart is implemented that if len(object.Parts) == 0, the whole object will be transited to Glacier as a single archive even if it's big. Further work is required if it should be transited to Glacier in parts.     
* It requires more work in serialization for LifecycleRule  
If a user put and get a lifecycle configuration as follows, he'll find the output seems not completely the same as saved.
It orients from LcRule definition in api/datatype/lifecycle.go .
I don't find any easy way to eliminate the difference.
```
cat lcfile.xml
<?xml version="1.0" ?> 
<LifecycleConfiguration>
        <Rule>
                <ID>LCRuleA</ID>
                <Status>Enabled</Status>
                <Expiration>
                        <Days>1</Days>
                </Expiration>
        </Rule>
s3cmd setlifecycle lcfile.xml s3://test-bucket1
s3cmd getlifecycle s3://test-bucket1
<?xml version="1.0" ?>
<LifecycleConfiguration>
	<Rule>
		<ID>LCRuleA</ID>
		<Filter>
			<Prefix/>                                  <------ No content here.
		</Filter>
		<Status>Enabled</Status>
		<Expiration>
			<Days>1</Days>
		</Expiration>
		<Transition/>                                <-------
	</Rule>
</LifecycleConfiguration>
```
## Future Work
* It'll be better if we check the checksum after transition to Glacier.  
But the fact is, for big file, we have the etag for the whole object, while Glacier backend checks only SHA256 tree hash for the whole object sent by yig.
To keep data integraty, it's better that yig can check the archive checksum.  
