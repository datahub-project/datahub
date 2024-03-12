# Working With Platform Instances

DataHub's metadata model for Datasets supports a three-part key currently: 
- Data Platform (e.g. urn:li:dataPlatform:mysql)
- Name (e.g. db.schema.name)
- Env or Fabric (e.g. DEV, PROD, etc.)

This naming scheme unfortunately does not allow for easy representation of the multiplicity of platforms (or technologies) that might be deployed at an organization within the same environment or fabric. For example, an organization might have multiple Redshift instances in Production and would want to see all the data assets located in those instances inside the DataHub metadata repository. 

As part of the `v0.8.24+` releases, we are unlocking the first phase of supporting Platform Instances in the metadata model. This is done via two main additions:
- The `dataPlatformInstance` aspect that has been added to Datasets which allows datasets to be associated to an instance of a platform
- Enhancements to all ingestion sources that allow them to attach a platform instance to the recipe that changes the generated urns to go from `urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,ENV)` format to `urn:li:dataset:(urn:li:dataPlatform:<platform>,<instance.name>,ENV)` format. Sources that produce lineage to datasets in other platforms (e.g. Looker, Superset etc) also have specific configuration additions that allow the recipe author to specify the mapping between a platform and the instance name that it should be mapped to. 


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/platform-instances-for-ingestion.png"/>
</p>


## Naming Platform Instances

When configuring a platform instance, choose an instance name that is understandable and will be stable for the foreseeable future. e.g. `core_warehouse` or `finance_redshift` are allowed names, as are pure guids like `a37dc708-c512-4fe4-9829-401cd60ed789`. Remember that whatever instance name you choose, you will need to specify it in more than one recipe to ensure that the identifiers produced by different sources will line up.

## Enabling Platform Instances

Read the Ingestion source specific guides for how to enable platform instances in each of them. 
The general pattern is to add an additional optional configuration parameter called `platform_instance`. 

e.g. here is how you would configure a recipe to ingest a mysql instance that you want to call `core_finance`
```yaml
source:
  type: mysql
  config:
    # Coordinates
    host_port: localhost:3306
    platform_instance: core_finance
    database: dbname
    
    # Credentials
    username: root
    password: example

sink:
  # sink configs
```


## 
