### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Architecture Support

The Db2 source is available on Windows x86_64, Linux x86_64, macOS x86_64, and macOS ARM.

It is not available on Linux ARM, and attempting to execute the source will result in an error.

#### Db2 Platform Support

This source has been tested against:

- Db2 for LUW using the default CLI driver (included in the `acryl-datahub[db2]` package).
- Db2 for z/OS using the default CLI driver (included in the `acryl-datahub[db2]` package). Note that, in some cases, this may [require Db2 Connect](https://www.ibm.com/support/pages/db2-odbc-cli-driver-download-and-installation-information).
- Db2 for IBM i (AS/400) using the IBM i Access ODBC Driver (not included and so must be installed separately by the end user; see [Using the IBM i Access ODBC Driver](#using-the-ibm-i-access-odbc-driver) below)

This source also includes nominal support for:

- Db2 for IBM i (AS/400) using the CLI driver ([requires Db2 Connect](https://www.ibm.com/support/pages/db2-odbc-cli-driver-download-and-installation-information))

#### Using the IBM i Access ODBC Driver

This source can connect to Db2 for IBM i (AS/400) directly using the IBM i Access ODBC Driver rather than using the default CLI driver that requires Db2 Connect.

This requires you to [install the IBM i Access ODBC Driver](https://ibmi-oss-docs.readthedocs.io/en/latest/odbc/installation.html) along with an ODBC driver manager such as `unixodbc`.

To use the IBM i Access ODBC Driver rather than the default driver, specify the `db2+pyodbc400` scheme in your ingestion configuration:

```yaml
scheme: "db2+pyodbc400"
```

If you're using [DataHub Cloud's Remote Executor](https://docs.datahub.com/docs/managed-datahub/remote-executor/about), you can build a custom image with the ODBC driver included. For instance, to build a custom image, you might use the following Dockerfile:

```dockerfile
FROM ${remote_executor_image} # Ask your DataHub account rep for the parent image name to use
USER root
RUN wget -P /etc/apt/sources.list.d/ https://public.dhe.ibm.com/software/ibmi/products/odbc/debs/dists/1.1.0/ibmi-acs-1.1.0.list
RUN apt-get update
RUN apt-get install -y ibm-iaccess
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
USER datahub
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
