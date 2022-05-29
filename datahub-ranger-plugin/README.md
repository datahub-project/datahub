---
title: "Configuring Apache Ranger"
hide_title: true
---
# Configuring Apache Ranger
Datahub Apache Ranger Plugin configuration is consist of below configuration sections 
1. Configuration at Apache Ranger Deployment 
2. Configuration at Datahub Deployment

The configuration sections are tested on [Privacera Platform](https://privacera.com/) v6.3.0.1.

# Assumption 
- Apache Ranger and DataHub are configured with same IDP
- Apache Ranger service is running on HTTP

# Configuration 
## Configuration at Apache Ranger Deployment 
1. Download the datahub-ranger-plugin from [Maven](https://mvnrepository.com/artifact/io.acryl/datahub-ranger-plugin)
2. Create  *datahub*  directory in each *Privacera Ranger* container 
```bash
docker exec privacera_ranger_1 mkdir ews/webapp/WEB-INF/classes/ranger-plugins/datahub
```
3. Copy the datahub-ranger-plugin jar into *Privacera Ranger* container
```bash 
docker cp datahub-ranger-plugin-<version>.jar privacera_ranger_1:/opt/ranger/ranger-2.1.0-admin/ews/webapp/WEB-INF/classes/ranger-plugins/datahub/
```
4. Download the [service definition](../datahub-ranger-plugin/conf/servicedef.json). This service definition is the ranger service definition JSON file for datahub-ranger-plugin-<version\>.jar
5. Execute below curl command to register the service definition <br /> 
Replace variables with corresponding values in curl command
- <ranger-admin-username\>
- <ranger-admin-password\>
- <ranger-host\>
```bash 
curl -u <ranger-admin-username>:<ranger-admin-password> -X POST -H "Accept: application/json" -H "Content-Type: application/json" --data @servicedef.json http://<ranger-host>:6080/service/public/v2/api/servicedef
```
6. Login to *Privacera Portal*. The  *datahub*  plugin should be available in  *Access Management -> Resource Policies*  section to create authorization policies for Datahub 
7. Create a service in *datahub plugin* with name *privacera_datahub*
8. Under service  *privacera_datahub*  you can create the policies to control DataHub authorization.
9. To allow  *platform*  access for DataHub root user i.e  *datahub* , perform the below steps 
   - Create a user  *datahub*
   - Create a policy under  *privacera_datahub*  service which should have a resource  *platform*  of resource type  *platform*  and allow all permissions to  *datahub*  user for the resource  *platform*


## Configuration at Datahub Deployment 
### Quickstart 
1. Download Apache Ranger security xml [ranger-datahub-security.xml](../datahub-ranger-plugin/conf/ranger-datahub-security.xml)
2. In  *ranger-datahub-security.xml*  edit the value of property  *ranger.plugin.datahub.policy.rest.url* 
```xml
    <property>
        <name>ranger.plugin.datahub.policy.rest.url</name>
        <value>http://[RANGER_SERVICE_HOST]:[RANGER_SERVICE_PORT]</value>
        <description>
            URL to Ranger Admin
        </description>
    </property>

```
3. Copy ranger-datahub-security.xml to  *~/.datahub/plugins/auth/resources/* . Create these directories if not exist
4. Disable DataHub default policy authorizer 
```bash
export AUTH_POLICIES_ENABLED=false
```
5. Enable Apache Ranger authorizer 
```bash
export RANGER_AUTHORIZER_ENABLED=true 
```
6. Set the Apache Ranger admin username 
```bash
export RANGER_USERNAME=<username>
```
7. Set the Apache Ranger admin password 
```bash
export RANGER_PASSWORD=<password>
```
8. Execute quickstart
```bash
datahub docker quickstart
```
9. Login into DataHub portal as  *datahub*  user. *datahub* user should get access to all DataHub resources as per defined policy in Privacera Portal.