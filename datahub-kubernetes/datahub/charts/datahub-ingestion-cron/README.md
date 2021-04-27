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
| crons | type | `{}` | A map of crawling parameters per different technology being crawler, the key in the object will be used as the name for the new cron job |
| crons.schedule | string | `"0 0 * * *"` | Cron expression (default is daily at midnight) for crawler jobs |
| crons.command | array | `["/bin/sh", "-c", "datahub ingest -c config.yml"]` | Array of strings denoting the crawling command to be invoked in the cron job. Provided example assumes a top-level file called `config.yml` has been mounted and is a valid [datahub crawling recipe](https://github.com/linkedin/datahub/tree/master/metadata-ingestion#recipes). |
| crons.hostAliases | array | `[]` | host aliases |
| crons.env | object | `{}` | Environment variables to add to the cronjob container |
| crons.envFromSecrets | object | `{}` | Environment variables from secrets to the cronjob container |
| crons.envFromSecrets*.secret | string | | secretKeyRef.name used for environment variable |
| crons.envFromSecrets*.key | string | | secretKeyRef.key used for environment variable |
| crons.extraVolumes | array | `[]` | Additional volumes to add to the pods |
| crons.extraVolumeMounts | array | `[]` | Additional volume mounts to add to the pods |
| crons.extraInitContainers | object | `{}` | Init containers to add to the cronjob container |
