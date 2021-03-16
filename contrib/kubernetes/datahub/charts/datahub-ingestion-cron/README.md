datahub-ingestion-cron
================
A Helm chart for datahub's metadata-ingestion framework with kerberos authentication.

## Chart Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| image.pullPolicy | string | `"Always"` | Image pull policy |
| image.repository | string | `"linkedin/datahub-ingestion"` | DataHub Ingestion image repository |
| image.tag | string | `"latest"` | DataHub Ingestion image tag |
| imagePullSecrets | array | `[]` (does not add image pull secrets to deployed pods) | Docker registry secret names as an array |
| labels | string | `{}` | Metadata labels to be added to each crawling cron job |
| crons | type | `[]` | A list of crawling parameters per different technology being crawler |
| crons.name | string | `crawler` | Name of the crawler container |
| crons.schedule | string | `""0 0 * * *"` | Cron expression (daily at midnight) for crawler jobs |
| crons.crawlerConfigPath | string | N/A | Path to metadata configuration file. This must explicitly defined as a mount and is **required**. |
| crons.hostAliases | array | `[]` | host aliases |
| crons.env | object | `{}` | Environment variables to add to the cronjob container |
| crons.envFromSecrets | object | `{}` | Environment variables from secrets to the cronjob container |
| crons.envFromSecrets*.secret | string | | secretKeyRef.name used for environment variable |
| crons.envFromSecrets*.key | string | | secretKeyRef.key used for environment variable |
| crons.extraVolumes | array | `[]` | Additional volumes to add to the pods |
| crons.extraVolumeMounts | array | `[]` | Additional volume mounts to add to the pods |
| crons.extraInitContainers | object | `{}` | Init containers to add to the cronjob container |
