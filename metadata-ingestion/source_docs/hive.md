# Hive

To install this plugin, run `pip install 'acryl-datahub[hive]'`.

This plugin extracts the following:

- List of databases, schema, and tables
- Column types associated with each table
- Detailed table and storage information

```yml
source:
  type: hive
  config:
    # For more details on authentication, see the PyHive docs:
    # https://github.com/dropbox/PyHive#passing-session-configuration.
    # LDAP, Kerberos, etc. are supported using connect_args, which can be
    # added under the `options` config parameter.
    #scheme: 'hive+http' # set this if Thrift should use the HTTP transport
    #scheme: 'hive+https' # set this if Thrift should use the HTTP with SSL transport
    username: user # optional
    password: pass # optional
    host_port: localhost:10000
    database: DemoDatabase # optional, defaults to 'default'
    # table_pattern/schema_pattern is same as above
    # options is same as above
```

<details>
  <summary>Example: using ingestion with Azure HDInsight</summary>

```yml
# Connecting to Microsoft Azure HDInsight using TLS.
source:
  type: hive
  config:
    scheme: "hive+https"
    host_port: <cluster_name>.azurehdinsight.net:443
    username: admin
    password: "<password>"
    options:
      connect_args:
        http_path: "/hive2"
        auth: BASIC
    # table_pattern/schema_pattern is same as above
```

</details>
