# MetadataChangeEvent (MCE) Consumer Job

## Starting job
Run below to start MCE consuming job.
```
./gradlew :metadata-jobs:mce-consumer-job:run
```
Create your own MCE to align the models in bootstrap_mce.dat.
Tips: one line per MCE with Python syntax.

Then you can produce MCE to feed your GMS.
```
cd metadata-ingestion && python mce_cli.py produce
```