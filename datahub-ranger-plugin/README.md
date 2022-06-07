---
title: "Configuring Authorization with Apache Ranger"
hide_title: true
---
# Configuring Authorization with Apache Ranger
DataHub integration with Apache Ranger allows DataHub Authorization policies to be controlled inside Apache Ranger.
Admins can create users, groups and roles on Apache Ranger, and then assign them to Ranger policies to control the authorization of requests to DataHub.

We'll break down configuration of the DataHub Apache Ranger Plugin into two parts:

1. Configuring your Apache Ranger Deployment
2. Configuring your DataHub Deployment

> Disclaimer: All configurations shown in this documented were tested against [Privacera Platform](https://privacera.com/) v6.3.0.1.

# Prerequisites 
- Apache Ranger and DataHub are configured for authentication via same IDP (either LDAP + JaaS or OIDC SSO)
- Apache Ranger service available via HTTP
- Basic authentication is enabled on Apache Ranger Service

# Configuration 

## Configuring your Apache Ranger Deployment

Perform the following steps to configure an Apache Ranger deployment to support creating access policies compatible with DataHub. 

1. Download the **datahub-ranger-plugin** from [Maven](https://mvnrepository.com/artifact/io.acryl/datahub-ranger-plugin)
2. Create a "datahub" directory inside the "ranger-plugins" directory where Apache Ranger is deployed. For example, to do this in a Privacera container
```bash
docker exec privacera_ranger_1 mkdir ews/webapp/WEB-INF/classes/ranger-plugins/datahub
```
3. Copy the downloaded **datahub-ranger-plugin** jar into the newly created "datahub" directory. For example, to do this in a Privacera container
```bash 
docker cp datahub-ranger-plugin-<version>.jar privacera_ranger_1:/opt/ranger/ranger-2.1.0-admin/ews/webapp/WEB-INF/classes/ranger-plugins/datahub/
```
4. Download the [service definition file](../datahub-ranger-plugin/conf/servicedef.json). This service definition is the ranger service definition JSON file for datahub-ranger-plugin-<version\>.jar
5. Register the downloaded service definition file with Apache Ranger Service. To do this executes the below curl command <br /> 
Replace variables with corresponding values in curl command
- <ranger-admin-username\>
- <ranger-admin-password\>
- <ranger-host\>
```bash 
curl -u <ranger-admin-username>:<ranger-admin-password> -X POST -H "Accept: application/json" -H "Content-Type: application/json" --data @servicedef.json http://<ranger-host>:6080/service/public/v2/api/servicedef
```
6. Login into the Privacera Portal to performs below steps. 
   1. Verify **datahub-ranger-plugin** is registered successfully: The  **datahub-ranger-plugin** should be visible as **DATAHUB**  in  *Access Management -> Resource Policies*. 
   2. Create a service under the plugin **DATAHUB** with name **ranger_datahub**

      **DATAHUB** plugin and **ranger_datahub** service is shown in below screenshot: <br/>
      
      ![Privacera Portal DATAHUB screenshot](./doc-images/datahub-plugin.png)

   3. Create policies under service **ranger_datahub** to control DataHub authorization.
   4. Allow DataHub root user, i.e. **datahub** to control DataHub services: To do this performs below steps
      - Create a user  **datahub**
      - Create a policy under **ranger_datahub** service which should have a resource  **platform**  of resource type  **platform**  and allow all permissions to  **datahub**  user for the resource  **platform**
      
      DataHub platform access policy screenshot: <br/>
      
      ![Privacera Portal DATAHUB screenshot](./doc-images/datahub-platform-access-policy.png)


## Configuring your DataHub Deployment

Perform the following steps to configure DataHub to send incoming requests to Apache Ranger for authorization.

1. Download Apache Ranger security xml [ranger-datahub-security.xml](../datahub-ranger-plugin/conf/ranger-datahub-security.xml)
2. In  **ranger-datahub-security.xml**  edit the value of property  *ranger.plugin.datahub.policy.rest.url*. Sample snippet is shown below
```xml
    <property>
        <name>ranger.plugin.datahub.policy.rest.url</name>
        <value>http://199.209.9.70:6080</value>
        <description>
            URL to Ranger Admin
        </description>
    </property>

```
3. Configuring DataHub to use Ranger Authorizer: Perform below steps on DataHub's host machine
   1. Create directory **~/.datahub/plugins/auth/resources/**: Executes below command
   ```bash
   mkdir -p ~/.datahub/plugins/auth/resources/
   ```
   2. Copy **ranger-datahub-security.xml** file to ~/.datahub/plugins/auth/resources/ 
   3. Disable DataHub default policy authorizer: Executes below command 
   ```bash
   export AUTH_POLICIES_ENABLED=false
   ```
   4. Enable Apache Ranger authorizer: Executes below command 
   ```bash
   export RANGER_AUTHORIZER_ENABLED=true 
   ```
   5. Set the Apache Ranger admin username: In below command replace <username\> by Apache Ranger Service username and execute the command
   ```bash
   export RANGER_USERNAME=<username>
   ```
   6. Set the Apache Ranger admin password: In below command replace <password\> by Apache Ranger Service user's password and execute the command 
   ```bash
   export RANGER_PASSWORD=<password>
   ```
   7. Redeploy the DataHub
4. Verify root **datahub** user is able to access DataHub Portal: Login to DataHub portal as **datahub** and **datahub** user should be able to access all the DataHub functionalities
