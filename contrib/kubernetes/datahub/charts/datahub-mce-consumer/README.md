datahub-mce-consumer
====================
A Helm chart for datahub-mce-consumer

Current chart version is `0.2.0`

## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| extraEnvs | Extra [environment variables][] which will be appended to the `env:` definition for the container | `[]` |
| extraVolumes | Templatable string of additional `volumes` to be passed to the `tpl` function | "" |
| extraVolumeMounts | Templatable string of additional `volumeMounts` to be passed to the `tpl` function | "" |
| fullnameOverride | string | `""` |  |
| global.kafka.bootstrap.server | string | `"broker:29092"` |  |
| global.kafka.schemaregistry.url | string | `"http://schema-registry:8081"` |  |
| global.datahub.gms.port | string | `"8080"` |  |
| global.hostAliases[0].hostnames[0] | string | `"broker"` |  |
| global.hostAliases[0].hostnames[1] | string | `"mysql"` |  |
| global.hostAliases[0].hostnames[2] | string | `"elasticsearch"` |  |
| global.hostAliases[0].hostnames[3] | string | `"neo4j"` |  |
| global.hostAliases[0].ip | string | `"192.168.0.104"` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"linkedin/datahub-mce-consumer"` |  |
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
| service.port | int | `80` |  |
| service.type | string | `"ClusterIP"` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `nil` |  |
| tolerations | list | `[]` |  |
