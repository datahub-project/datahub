### Using the IBM i Access ODBC Driver

This source can connect to Db2 for IBM i (AS/400) directly using the IBM i Access ODBC Driver rather than using the default CLI driver that requires Db2 Connect.

This requires you to [install the IBM i Access ODBC Driver](https://ibmi-oss-docs.readthedocs.io/en/latest/odbc/installation.html) along with an ODBC driver manager such as `unixodbc`.

To use the IBM i Access ODBC Driver rather than the default driver, specify the `db2+pyodbc400` scheme in your ingestion configuration:

```yaml
scheme: "db2+pyodbc400"
```

If you're using [DataHub Cloud's Remote Executor](https://docs.datahub.com/docs/managed-datahub/remote-executor/about), you can either build a custom image with the ODBC driver included, or sideload the driver. For instance, to sideload the driver and when using the Remote Executor Helm chart, you might use the following Helm values file:

```yaml
extraInitContainers:
  - name: init-db2-as400-driver
    image: debian:stable-slim
    command:
      - bash
      - -c
      - |
        set -euxo pipefail
        apt-get update
        apt-get install -y wget
        wget -P /etc/apt/sources.list.d/ https://public.dhe.ibm.com/software/ibmi/products/odbc/debs/dists/1.1.0/ibmi-acs-1.1.0.list
        apt-get update
        apt-get install -y ibm-iaccess
        cp /etc/odbcinst.ini /opt/ibm
    volumeMounts:
      - name: ibmdriver
        mountPath: "/opt/ibm"

extraVolumes:
  - name: ibmdriver
    emptyDir: {}

extraVolumeMounts:
  - name: ibmdriver
    mountPath: "/opt/ibm"
    readOnly: true

nodeSelector:
  kubernetes.io/arch: amd64
```

Then configure your source to set the following environment variables:

```bash
LD_LIBRARY_PATH="/opt/ibm/iaccess/lib64/"
ODBCSYSINI="/opt/ibm"
```
