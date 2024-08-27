# Using Kubernetes

If you have deployed DataHub using our official [helm charts](https://github.com/acryldata/datahub-helm) you can use the 
datahub ingestion cron subchart to schedule ingestions. 

Here is an example of what that configuration would look like in your **values.yaml**:

```yaml
datahub-ingestion-cron:
  enabled: true
  crons:
    mysql:
      schedule: "0 * * * *" # Every hour
      recipe:
        configmapName: recipe-config
        fileName: mysql_recipe.yml
```

This assumes the pre-existence of a Kubernetes ConfigMap which holds all recipes being scheduled in the same namespace as
where the cron jobs will be running.

An example could be:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: recipe-config
data:
  mysql_recipe.yml: |-
    source:
      type: mysql
      config:
        # Coordinates
        host_port: <MYSQL HOST>:3306
        database: dbname
    
        # Credentials
        username: root
        password: example
    
    sink:
      type: datahub-rest
      config:
        server: http://<GMS_HOST>:8080
```

For more information, please see the [documentation](https://github.com/acryldata/datahub-helm/tree/master/charts/datahub/subcharts/datahub-ingestion-cron) of this sub-chart.