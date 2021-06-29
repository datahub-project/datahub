datahub-ingestion-cron
================
A Helm chart for datahub's metadata-ingestion framework with kerberos authentication.

## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| image.pullPolicy | string | `"Always"` | Image pull policy |
| image.repository | string | `"linkedin/datahub-ingestion"` | DataHub Ingestion image repository |
| image.tag | string | `"v0.8.3"` | DataHub Ingestion image tag |
| imagePullSecrets | array | `[]` (does not add image pull secrets to deployed pods) | Docker registry secret names as an array |
| labels | string | `{}` | Metadata labels to be added to each crawling cron job |
| crons | type | `{}` | A map of crawling parameters per different technology being crawler, the key in the object will be used as the name for the new cron job |
| crons.schedule | string | `"0 0 * * *"` | Cron expression (default is daily at midnight) for crawler jobs |
| crons.recipe | object | `{}` | Recipe configuration to be executed (required) |
| crons.recipe.configmapName | string | `""` | Name of configmap to be mounted containing recipe to be executed |
| crons.recipe.fileName | string | `""` | Name of property within configMap referenced by `recipe.configName` with the concrete recipe definition |
| crons.command | array | `["/bin/sh", "-c", "datahub ingest -c /etc/recipe/<crons.recipe.fileName>"]` | Array of strings denoting the crawling command to be invoked in the cron job. By default it will execute the recipe defined in the `crons.recipe` object. Cron crawling customization is possible by having extra volumes with custom logic to be executed. |
| crons.hostAliases | array | `[]` | host aliases |
| crons.env | object | `{}` | Environment variables to add to the cronjob container |
| crons.envFromSecrets | object | `{}` | Environment variables from secrets to the cronjob container |
| crons.envFromSecrets*.secret | string | | secretKeyRef.name used for environment variable |
| crons.envFromSecrets*.key | string | | secretKeyRef.key used for environment variable |
| crons.extraVolumes | array | `[]` | Additional volumes to add to the pods |
| crons.extraVolumeMounts | array | `[]` | Additional volume mounts to add to the pods |
| crons.extraInitContainers | object | `{}` | Init containers to add to the cronjob container |
