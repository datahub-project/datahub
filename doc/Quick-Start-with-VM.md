> We're in the process of replacing the VM image with docker images. Stay tuned!

For the ease of exploring the features of WhereHows, we prepared a VM image based on [Cloudera Quick Start VM for CDH 5.4.x](#http://www.cloudera.com/content/cloudera/en/downloads/quickstart_vms/cdh-5-4-x.html).

In this VM, we prepared some sample data on HDFS, and set up Azkaban and Oozie to run several sample jobs.
And we run the WhereHows Service to collect metadata from these systems, so you can easily try WhereHows within minutes.


# Steps to exploring the WhereHows application
1. Download and install the latest version of [VMware Player][vmplayer], for example VMware Player 12.0.

2. Download the all partitions of image in [WhereHows][WhereHows]. (If you are in China, download from here [WhereHows china link](http://pan.baidu.com/s/1qXi2XWg) )

3. Extract by using ```7za e WhereHows-VM.7z.001```

4. Start VM and run the image. Check VM is running correctly following <a href="#check-VM">appendix</a>

5. The WhereHows frontend service should auto start. Open a browser, type: **http://localhost:9008**

6. Have fun!

# Steps to start the backend service
1. Start the WhereHows Backend Service:

    ```
    cd ~/wherehows;
    ./runbackend;
    ```
2. There are already some jobs set up in VM. You can also add new jobs following the instruction in [Set Up New Metadata ETL Jobs](Set Up New Metadata ETL Jobs).

3. If you want to start the Teradata metadata ETL job, you need to first set up a Teradata VM.

  a. Download the image of [Teradata][Teradata]. (TDExpress15.00.02_Sles11_40GB is a good choice as it contains some sample data.)

  b. Start a VM of Teradata following the Teradata instructions.

  c. Add the Teradata configuration as [Teradata ETL instruction](Teradata-Dataset). (We already have some sample configuration in VM, you need to change to your configuration).
  
  d. Schedule the job through [etl-job-schedule API](Backend-API#etl-job-schedule). 
  
  e. After the job finished, check the result from UI.

4. To test whether the backend service have run successfully, you can add some dataset to hdfs (in avro format) and/or run some pig/hive/mapreduce jobs that read/write to some datasets. Run correspond jobs again and check UI.
 Example of test backend lineage/execution metadata ETL:
  1.  open azkaban : localhost:8081, login as azkaban, run the sample jobs of 'twitter' and 'retail'
  2.  make sure the backend service is running : ```cd ~/wherehows; ./runbackend```
  3.  activate the `AZKABAN_LINEAGE_METADATA_ETL` job and `AZKABAN_EXECUTION_METADATA_ETL` job by API : 
```
curl -H "Content-Type: application/json" -X PUT -d '{"wh_etl_job_name":"AZKABAN_LINEAGE_METADATA_ETL", "ref_id":1, "control":"activate"}' http://localhost:19001/etl/control 
```
``` 
curl -H "Content-Type: application/json" -X PUT -d '{"wh_etl_job_name":"AZKABAN_EXECUTION_METADATA_ETL", "ref_id":1, "control":"activate"} 'http://localhost:19001/etl/control 
```
The schedule will run the back end job in next check, you can also shut the backend service down and bring it back again so it will immediately run these jobs.
   You can check the backend log by ```tail -f /var/tmp/wherehows/wherehows.log```. After you see these two jobs have been finished, refresh the wherehows page, you should be able to see the lineage.

# Update to the newest version
As the project is under active development, there are always new changes and bug fix. We will periodically update the VM images, but you definitely don't want to download it again and again. Here is the simple steps to update WhereHows on your VM.
## update backend models:
  1. `git pull` update to latest version
  2. `cd backend-service; gradle build; gradle dist` build backend models
  3. `scp target/universal/backend-service-1.0-SNAPSHOT.zip cloudera@$VM_HOST:~/wherehows/` cp the zip file to your VM.
  4. On your VM: `cd wherehows; rm -rf backend-service-1.0-SNAPSHOT; unzip backend-service-1.0-SNAPSHOT.zip` update to newest version
  5. `kill -9 $current_wherehows_backend_pid; ./runbackend` restart the backend service.

## update frontend models:
  1. `git pull` update to latest version
  2. `cd web; gradle build; gradle dist` build frontend models
  3. `scp target/universal/wherehows-1.0-SNAPSHOT.zip cloudera@$VM_HOST:~/wherehows/` cp the zip file to your VM.
  4. On your VM: `cd wherehows; rm -rf wherehows-1.0-SNAPSHOT; unzip wherehows-1.0-SNAPSHOT.zip` update to newest version
  5. `kill -9 $current_wherehows_frontend_pid; ./runfrontend` restart the backend service.


<a name="check-VM" />

# Appendix: Check all service running on VM correctly
To make sure all the Hadoop services are running, refer to /root/bin/enable-all-hadoop-service.sh

```
chkconfig mysqld on

chkconfig hadoop-hdfs-namenode on
chkconfig hadoop-yarn-resourcemanager on
chkconfig hadoop-hdfs-secondarynamenode on
chkconfig hadoop-yarn-nodemanager on
chkconfig hadoop-hdfs-datanode on
chkconfig hadoop-mapreduce-historyserver on

chkconfig hue on
chkconfig oozie on
chkconfig hive-metastore  on
chkconfig hive-server2 on
chkconfig zookeeper-server on
chkconfig azkaban on
chkconfig wherehows-frontend on

chkconfig cloudera-scm-agent off
chkconfig cloudera-scm-server off
```

## Enjoy!

[vmplayer]: https://www.vmware.com/products/player
[WhereHows]: https://linkedin.box.com/wherehows-demo-in-cloudera-vm
[Teradata]: https://downloads.teradata.com/download/database/teradata-express-for-vmware-player