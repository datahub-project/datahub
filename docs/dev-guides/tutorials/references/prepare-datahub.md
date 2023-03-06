# Prepare Local Datahub Environment

## Deploy Datahub Quickstart 

You need a datahub running in local environment. First, install acryl-datahub if you did't yet. 
```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
```
If you can see datahub version like this, you're good to go. 
```shell
$ datahub version
DataHub CLI version: 0.10.0.1
Python version: 3.9.6 (default, Jun 16 2022, 21:38:53)
[Clang 13.0.0 (clang-1300.0.27.3)]
```

Run datahub quickstart. This will deploy local datahub server to http://localhost:9002 
```shell
datahub docker quickstart
```
After logging in with the default credential(`username: datahub / password: datahub`), you can see Datahub ready for you. 

![datahub-main-ui](../../../imgs/tutorials/datahub-main-ui.png)

Please refer to [DataHub Quickstart Guide](https://datahubproject.io/docs/quickstart) for more information. 

## Ingest Sample Data
We will use sample data provided with datahub quickstart. 
If you already have data on your datahub, you might skip this part. 

```shell
datahub docker ingest-sample-data 
```
This will ingest various entities like datasets, terms and tags to your local Datahub.
![datahub-main-ui](../../../imgs/tutorials/sample-ingestion.png)

Now you're ready to start tutorials! 