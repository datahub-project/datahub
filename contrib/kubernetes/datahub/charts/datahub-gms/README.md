datahub-gms
===========
A Helm chart for LinkedIn DataHub's datahub-gms component

Current chart version is `0.1.0`

## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| fullnameOverride | string | `"datahub-gms-deployment"` |  |
| global.datahub.appVersion | string | `"1.0"` |  |
| global.datahub.gms.host | string | `"datahub-gms-service"` |  |
| global.datahub.gms.port | string | `"8080"` |  |
| global.datahub.gms.secret | string | `"YouKnowNothing"` |  |
| global.elasticsearch.host | string | `"192.168.0.104"` |  |
| global.elasticsearch.port | string | `"9200"` |  |
| global.hostAliases[0].hostnames[0] | string | `"broker"` |  |
| global.hostAliases[0].ip | string | `"192.168.0.104"` |  |
| global.kafka.bootstrap.server | string | `"192.168.0.104:29092"` |  |
| global.kafka.schemaregistry.url | string | `"http://192.168.0.104:8081"` |  |
| global.neo4j.password | string | `"datahub"` |  |
| global.neo4j.uri | string | `"bolt://192.168.0.104"` |  |
| global.neo4j.username | string | `"neo4j"` |  |
| global.sql.datasource.driver | string | `"com.mysql.jdbc.Driver"` |  |
| global.sql.datasource.host | string | `"192.168.0.104:3306"` |  |
| global.sql.datasource.password | string | `"datahub"` |  |
| global.sql.datasource.url | string | `"jdbc:mysql://192.168.0.104:3306/datahub?verifyServerCertificate=false\u0026useSSL=true"` |  |
| global.sql.datasource.username | string | `"datahub"` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"linkedin/datahub-gms"` |  |
| image.tag | string | `"latest"` |  |
| imagePullSecrets | list | `[]` |  |
| ingress.annotations | object | `{}` |  |
| ingress.enabled | bool | `false` |  |
| ingress.hosts[0].host | string | `"chart-example.local"` |  |
| ingress.hosts[0].paths | list | `[]` |  |
| ingress.tls | list | `[]` |  |
| nameOverride | string | `""` |  |
| nodeSelector | object | `{}` |  |
| podSecurityContext | object | `{}` |  |
| replicaCount | int | `1` |  |
| resources | object | `{}` |  |
| securityContext | object | `{}` |  |
| service.port | int | `8080` |  |
| service.type | string | `"LoadBalancer"` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `nil` |  |
| tolerations | list | `[]` |  |
