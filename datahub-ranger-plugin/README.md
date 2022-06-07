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

### Defining a Ranger Policy

Now, you should have the DataHub plugin registered with Apache Ranger. Next, we'll create a sample user and add them to our first resource policy.

1. Login into the Apache Ranger UI (Privacera Portal) to performs below steps. 
2. Verify **datahub-ranger-plugin** is registered successfully: The  **datahub-ranger-plugin** should be visible as **DATAHUB**  in  *Access Management -> Resource Policies*. 
3. Create a service under the plugin **DATAHUB** with name **ranger_datahub**

      **DATAHUB** plugin and **ranger_datahub** service is shown in below screenshot: <br/>
      
      ![Privacera Portal DATAHUB screenshot](./doc-images/datahub-plugin.png)

4. Create a new policy under service **ranger_datahub** - this will be used to control DataHub authorization. 
5. Create a test user & assign them to a policy. We'll use the `datahub` user, which is the default root user inside DataHub.

   To do this performs below steps
      - Create a user  **datahub** 
      - Create a policy under **ranger_datahub** service. To assign [Platform Privileges](https://datahubproject.io/docs/policies/) (e.g. Admin privileges), simply use the "platform" resource type which is defined. To test the flow, we can simply assign the **datahub** user all platform privileges that are available through the Ranger UI. This will enable the "datahub" to have full platform admin privileges. 

     > To define fine-grained resource privileges, e.g. for DataHub Datasets, Dashboards, Charts, and more, you can simply select the appropriate Resource Type in the Ranger policy builder. You should also see a list of privileges that are supported for each resource type, which correspond to the actions that you can perform. To learn more about supported privileges, check out the DataHub [Policies Guide](https://datahubproject.io/docs/policies/). 
      
      DataHub platform access policy screenshot: <br/>
      
      ![Privacera Portal DATAHUB screenshot](./doc-images/datahub-platform-access-policy.png)

Once we've created our first policy, we can set up DataHub to start authorizing requests using Ranger policies. 


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
3. Configure DataHub to use a Ranger **Authorizer**. On the host where `datahub-gms` is deployed, follow these steps:
   1. Create directory **~/.datahub/plugins/auth/resources/**: Executes below command
   ```bash
   mkdir -p ~/.datahub/plugins/auth/resources/
   ```
   2. Copy **ranger-datahub-security.xml** file to ~/.datahub/plugins/auth/resources/ 
   3. [Optional] Disable the DataHub default policy authorizer by setting the following environment variable on the `datahub-gms` container: 
   ```bash
   export AUTH_POLICIES_ENABLED=false
   ```
   4. Enable the Apache Ranger authorizer by setting the following environment variable on the `datahub-gms` container: 
   ```bash
   export RANGER_AUTHORIZER_ENABLED=true 
   ```
   5. Set the Apache Ranger admin username by setting the following environment variable on the `datahub-gms` container:
   ```bash
   export RANGER_USERNAME=<username>
   ```
   6. Set the Apache Ranger admin username by setting the following environment variable on the `datahub-gms` container:
   ```bash
   export RANGER_PASSWORD=<password>
   ```
   7. Redeploy DataHub (`datahub-gms`) with the new environment variables 

That's it! Now we can test out the integration. 

### Validating your Setup
To verify that things are working as expected, we can test that the root **datahub** user has all Platform Privileges and is able to perform all operations: managing users & groups, creating domains, and more. To do this, simply log into your DataHub deployment via the root DataHub user. 
