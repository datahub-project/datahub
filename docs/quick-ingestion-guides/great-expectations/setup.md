---
title: Setup
---
# Great Expectations Guide: Setup 

This setup guide will walk you through the steps you'll need to take via your Datahub Actions container/pod.

## Great Expectations SetUp

* Access must be done with root user
* Installation of great_expectations in datahub actions: `pip install 'acryl-datahub[great_expectations]'`

## Great Expectations Special Requisites 

If you are planning on create assertion tests over Hive data, you will have to fulfill some extra requisites:

* Installation of kerberos (this step would only be needed if Kerberos is needed in the Actions-Hive connection): `pip install kerberos`
* Obtain a kerberos ticket with a user with enough privileges to connect to Hive: `kinit <user>@<realm>`
* Installation of thrift 0.13.0 (as in newer versions there is a bug sending the headers): `pip install thrift==0.13.0`
* Installation of great_expectations 0.15.43 (as in lower versions, Hive connection is not supported): `pip install great-expectations==0.15.43`

## Next Steps

Once you've confirmed all of the above, it's time to [move on](configuration.md) to configure the files needed.

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*