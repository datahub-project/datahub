---
title: Setup
---
# Great Expectations Guide: Setup 

This setup guide will walk you through the steps you'll need to take via your Datahub Actions container/pod.

## Great Expectations SetUp

* Port 8888 must be exposed in actions container/pod in order to access Jupyter notebooks created in future steps.
* Access must be done with root user.
* Installation of great_expectations in datahub actions: `pip install 'acryl-datahub[great_expectations]'`.
* Initialize great expectations (creation of new data context, which means creating a new directory and files inside it): `great_expectations init`.

## Great Expectations Special Requisites 

If you are planning on create assertion tests over Hive data, you will have to fulfill some extra requisites:

* Installation of kerberos (this step would only be needed if Kerberos is needed in the Actions-Hive connection): `pip install kerberos`.
* Installation of thrift 0.13.0 (as in newer versions there is a bug sending the headers): `pip install thrift==0.13.0`.
* Installation of great_expectations 0.15.43 (as in lower versions, Hive connection is not supported): `pip install great-expectations==0.15.43`.
* Obtain a kerberos ticket with a user with enough privileges to connect to Hive: `kinit <user>@<realm>`.

:::note
The initialization of great_expectations should be done once Great Expectations (with its corresponding version) has been installed.
:::

## Next Steps

Once you've confirmed all of the above, it's time to [move on](configuration.md) to configure the files needed.

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*