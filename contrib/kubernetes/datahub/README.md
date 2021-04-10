datahub
=======
A Helm chart for LinkedIn DataHub

Current chart version is `0.1.2`

#### Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| datahub-frontend.enabled | bool | `true` |  |
| datahub-frontend.image.repository | string | `"linkedin/datahub-frontend"` |  |
| datahub-frontend.image.tag | string | `"latest"` |  |
| datahub-gms.enabled | bool | `true` |  |
| datahub-gms.image.repository | string | `"linkedin/datahub-gms"` |  |
| datahub-gms.image.tag | string | `"latest"` |  |
| datahub-mae-consumer.enabled | bool | `true` |  |
| datahub-mae-consumer.image.repository | string | `"linkedin/datahub-mae-consumer"` |  |
| datahub-mae-consumer.image.tag | string | `"latest"` |  |
| datahub-mce-consumer.enabled | bool | `true` |  |
| datahub-mce-consumer.image.repository | string | `"linkedin/datahub-mce-consumer"` |  |
| datahub-mce-consumer.image.tag | string | `"latest"` |  |
| datahub-ingestion-cron.enabled | bool | `false` | |
| elasticsearchSetupJob.enabled | bool | `true` | |
| elasticsearchSetupJob.image.repository | string | `"linkedin/datahub-elasticsearch-setup"` |  |
| elasticsearchSetupJob.image.tag | string | `"latest"` |  |
| kafkaSetupJob.enabled | bool | `true` | |
| kafkaSetupJob.image.repository | string | `"linkedin/datahub-kafka-setup"` |  |
| kafkaSetupJob.image.tag | string | `"latest"` |  |
| mysqlSetupJob.enabled | bool | `false` | |
| mysqlSetupJob.image.repository | string | `""` |  |
| mysqlSetupJob.image.tag | string | `""` |  |
| global.datahub.appVersion | string | `"1.0"` |  |
| global.datahub.gms.port | string | `"8080"` |  |
| global.elasticsearch.host | string | `"elasticsearch"` |  |
| global.elasticsearch.port | string | `"9200"` |  |
| global.hostAliases[0].hostnames[0] | string | `"broker"` |  |
| global.hostAliases[0].hostnames[1] | string | `"mysql"` |  |
| global.hostAliases[0].hostnames[2] | string | `"elasticsearch"` |  |
| global.hostAliases[0].hostnames[3] | string | `"neo4j"` |  |
| global.hostAliases[0].ip | string | `"192.168.0.104"` |  |
| global.kafka.bootstrap.server | string | `"broker:29092"` |  |
| global.kafka.zookeeper.server | string | `"zookeeper:2181"` |  |
| global.kafka.schemaregistry.url | string | `"http://schema-registry:8081"` |  |
| global.neo4j.host | string | `"neo4j:7474"` |  |
| global.neo4j.uri | string | `"bolt://neo4j"` |  |
| global.neo4j.username | string | `"neo4j"` |  |
| global.neo4j.password.secretRef | string | `"neo4j-secrets"` |  |
| global.neo4j.password.secretKey | string | `"neo4j-password"` |  |
| global.sql.datasource.driver | string | `"com.mysql.jdbc.Driver"` |  |
| global.sql.datasource.host | string | `"mysql:3306"` |  |
| global.sql.datasource.hostForMysqlClient | string | `"mysql"` |  |
| global.sql.datasource.url | string | `"jdbc:mysql://mysql:3306/datahub?verifyServerCertificate=false\u0026useSSL=true"` |  |
| global.sql.datasource.username | string | `"datahub"` |  |
| global.sql.datasource.password.secretRef | string | `"mysql-secrets"` |  |
| global.sql.datasource.password.secretKey | string | `"mysql-password"` |  |

#### Optional Chart Values

| global.credentialsAndCertsSecretPath | string | `"/mnt/certs"` |  |
| global.credentialsAndCertsSecrets.name | string | `""` |  |
| global.credentialsAndCertsSecrets.secureEnv | string | `""` |  |
| global.springKafkaConfigurationOverrides | string | `""` |  |
