---
description: >-
  Advanced techniques for ingestion, not recommended for companies running a
  POC.
---

# Advanced Ingestion

To review our guide on general ingestion, click [here](metadata-ingestion/README.md).

## Real-Time Ingestion&#x20;

Real time ingestion means pushing metadata into DataHub as it is produced. The metadata that can be captured this way primarily includes metadata gathered from individual runs of data pipelines, such as lineage edges.&#x20;

The primary integration supported along the real time path is Airflow. The process of emitting metadata from Airflow is covered in detail within [Lineage with Airflow](metadata-ingestion/README.md#lineage-with-airflow).&#x20;

Note: If you are using Astronomer Cloud, you can't currently use the airflow CLI to create connections. You would need to create the connection in the airflow UI running on astro cloud. You also could add the connection URI as an environment variable in the astronomer UI. Here are a few resources:

- [Creating connections with the UI](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui)
- [Storing connections in environment variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#storing-a-connection-in-environment-variables)
- [Environment Variables on Astronomer](https://www.astronomer.io/docs/cloud/stable/deploy/environment-variables)

DataHub users are also free to push metadata themselves using the Python Emitter Library. This can be used to develop metadata workflows custom to your organization.&#x20;

### Configuring Airflow's DataHub Connection (UI)&#x20;

Inside the Airflow UI, create a new connection with destination of DataHubs REST API. You'll need to set&#x20;

- Host / server endpoint: `https://<account name>.acryl.io/gms`
- Password: `<API key>`

![Airflow 2.x Connection Setup UI](../imgs/saas/image-(9).png)

### Configuring an Emitter

With the [REST emitter,](metadata-ingestion/README.md#using-as-a-library-(sdk).md) just provide your API key through the token parameter.

```python
from datahub.emitter.rest_emitter import DatahubRestEmitter

ACCOUNT_NAME = "<account name>"
ACRYL_API_KEY = "<API key>"

mce = ...  #construct an MCE however you'd like

emitter = DatahubRestEmitter(
    f"https://{ACCOUNT_NAME}.acryl.io/gms",
    token=ACRYL_API_KEY,
)

emitter.emit_mce(mce)
```
