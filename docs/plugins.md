# Plugins Guide
Plugins are way to enhance the basic DataHub functionality in a custom manner.

The listed functionalities of DataHub can be customized.

- [Authentication](#authentication)
- [Authorization](#authorization)

Refer their individual section for enhancing basic functionality of DataHub.

><span style="color:red">Note</span>: This is in <b>BETA</b> version

## Authentication 
The sample authenticator implementation can be found at [Authenticator Sample](../metadata-service/plugin/src/test/sample-test-plugins)

Follow below steps to implement a custom authenticator 
1. Add _datahub-auth-api_ as implementation dependency: Maven coordinates of _datahub-auth-api_ can be found at [Maven](https://mvnrepository.com/artifact/io.acryl/datahub-auth-api) 
<br/>Example of gradle dependency is given below.
   ```groovy
    dependencies {
        implementation 'io.acryl:datahub-auth-api:0.8.45'
    }
   ```
2. Implements the Authenticator interface: Refer [Authenticator Sample](../metadata-service/plugin/src/test/sample-test-plugins)
<br/> Example code of class which implements the Authenticator interface is given below 
    ```java
    public class GoogleAuthenticator implements Authenticator {
   
        @Override
        public void init(@Nonnull Map<String, Object> authenticatorConfig, @Nullable AuthenticatorContext context) {
          // Plugin initialization code will go here 
          // DataHub will call this method on boot time
        }
   
        @Nullable
        @Override
        public Authentication authenticate(@Nonnull AuthenticationRequest authenticationRequest)
            throws AuthenticationException {
            // DataHub will call this method whenever authentication decisions are need to be taken
            // Authenticate the request and return Authentication
        }
    }
   ```
3. Create an uber jar: Use `com.github.johnrengelman.shadow` gradle plugin to create an uber jar. Refer build.gradle file of [Apache Ranger Plugin](../metadata-auth/apache-ranger-plugin) for reference
<br/> Important statements from build.gradle 
   ```groovy
     apply plugin: 'com.github.johnrengelman.shadow';
     shadowJar {
         // Exclude com.datahub.plugins package and files related to jar signature   
         exclude "com/datahub/plugins/", "META-INF/*.RSA", "META-INF/*.SF","META-INF/*.DSA"
     }
   ```
4. Refer section [Plugin Installation](#plugin-installation) for plugin installation in DataHub environment
5. Refer section [Plugin Permissions](#plugin-permissions) for permissions a plugin has while executing in DataHub environment


## Authorization
The sample authorizer implementation can be found at [Authorizer Sample](../metadata-auth/apache-ranger-plugin)

Follow below steps to implement a custom authorizer

1. Add _datahub-auth-api_ as implementation dependency: Maven coordinates of _datahub-auth-api_ can be found at [Maven](https://mvnrepository.com/artifact/io.acryl/datahub-auth-api)
2. Implements the Authorizer interface: [Authorizer Sample](../metadata-auth/apache-ranger-plugin)
   <br/> Example code of class which implements the Authenticator interface is given below
   ```java
    public class ApacheRangerAuthorizer implements Authorizer {

        @Override
        public void init(@Nonnull Map<String, Object> authorizerConfig, @Nonnull AuthorizerContext ctx) {
          // Plugin initialization code will go here 
          // DataHub will call this method on boot time
        }

        @Override
        public AuthorizationResult authorize(@Nonnull AuthorizationRequest request) {
            // DataHub will call this method whenever authorization decisions are need be taken
            // Authorize the request and return AuthorizationResult
        }

        @Override
        public AuthorizedActors authorizedActors(String privilege, Optional<ResourceSpec> resourceSpec) {
            // Need to add doc
        }
    }
   ```
3. Create an uber jar: Use `com.github.johnrengelman.shadow` gradle plugin to create an uber jar. Refer build.gradle file of [Apache Ranger Plugin](../metadata-auth/apache-ranger-plugin) for reference
   <br/> Important statements from build.gradle
   ```groovy
     apply plugin: 'com.github.johnrengelman.shadow';
     shadowJar {
         // Exclude com.datahub.plugins package and files related to jar signature   
         exclude "com/datahub/plugins/", "META-INF/*.RSA", "META-INF/*.SF","META-INF/*.DSA"
     }
   ```
4. Refer section (Plugin Installation)[#plugin_installation] for plugin installation in DataHub environment
5. Refer section (Plugin Permissions)[#plugin_permissions] for permissions a plugin has while executing in DataHub environment

## Plugin Installation
DataHub's GMS Service searches for the plugins in container's local directory at location `/etc/datahub/plugins/auth/`. This location will be referred as `plugin-base-directory` hereafter.

For docker, we set docker-compose to mount ${HOME}/.datahub directory to /etc/datahub directory within the GMS containers. For kubernetes you need to mount a volume at /etc/datahub.

Follow below steps to install plugins:

Lets consider you have created an uber jar for authorizer plugin and jar name is apache-ranger-authorizer.jar and class com.abc.RangerAuthorizer has implemented the [Authorizer](../metadata-auth/auth-api/src/main/java/com/datahub/plugins/auth/authorization/Authorizer.java) interface.

1. Create a plugin configuration file: Create a `config.yml` file at `${HOME}/.datahub/plugins/auth/`. For more detail on configuration refer [Config Detail](#config-detail) section
2. Create a plugin directory: Create plugin directory as `apache-ranger-authorizer`, this directory will be referred as `plugin-home` hereafter

   ```shell
    mkdir -p ${HOME}/.datahub/plugins/auth/apache-ranger-authorizer
   ```
3. Copy plugin jar to `plugin-home`: Copy `apache-ranger-authorizer.jar` to `plugin-home`

   ```shell
    copy apache-ranger-authorizer.jar ${HOME}/.datahub/plugins/auth/apache-ranger-authorizer
   ```
4. Update plugin configuration file: Add below entry in `config.yml` file, consider plugin takes username and password as configuration
   ```yaml
      plugins:
        - name: "apache-ranger-authorizer"
          type: "authorizer"
          enabled: "true"
          params:
            className: "com.abc.RangerAuthorizer"
            configs:
               username: "foo"
               password: "fake"
   
   ```
5. Restart datahub-gms container: On startup DataHub GMS service performs below steps 
   1. Load `config.yml`
   2. Prepare list of plugin where `enabled` is set to `true`
   3. Look for directory equivalent to plugin `name` in `plugin-base-directory`. In this case it is `/etc/datahub/plugins/auth/apache-ranger-authorizer/`, this directory will become `plugin-home` 
   4. Look for `params.jarFileName` attribute otherwise look for jar having name as &lt;plugin-name&gt;.jar. In this case  it is `/etc/datahub/plugins/auth/apache-ranger-authorizer/apache-ranger-authorizer.jar`
   5. Load class given in plugin `params.className` attribute from the jar, here load class `com.abc.RangerAuthorizer` from `apache-ranger-authorizer.jar`
   6. Call `init` method of plugin

By default, authentication is disabled in DataHub GMS, Please follow section [Enable GMS Authentication](#enable-gms-authentication) to enable authentication.


## Config Detail 
A sample `config.yml` can be found at [config.yml](../metadata-service/plugin/src/test/resources/valid-base-plugin-dir1/config.yml).

`config.yml` structure:

| Field                        | Required | Type                            | Default                         | Description                                                                             |
|------------------------------|----------|---------------------------------|---------------------------------|-----------------------------------------------------------------------------------------|
| plugins[].name               | ✅        | string                          |                                 | name of the plugin                                                                      |
| plugins[].type               | ✅        | enum[authenticator, authorizer] |                                 | type of plugin, possible values are authenticator or authorizer                         |
| plugins[].enabled            | ✅        | boolean                         |                                 | whether this plugin is enabled or disabled. DataHub GMS wouldn't process disabled plugin |  
| plugins[].params.className   | ✅        | string                          |                                 | Authenticator or Authorizer implementation class' fully qualified class name            |  
| plugins[].params.jarFileName |          | string                          | default to `plugins[].name`.jar | jar file name in `plugin-home`                                                          |  
| plugins[].params.configs     |          | map<string,object>              | default to empty map            | Runtime configuration required for plugin                                               |  

> plugins[] is an array of plugin, where you can define multiple authenticator and authorizer plugins. plugin name should be unique in plugins array.

## Enable GMS Authentication
By default authentication is disabled in DataHub GMS.

Follow below steps to enable GMS authentication
1. Download docker-compose.quickstart.yml: Download docker compose file [docker-compose.quickstart.yml](../docker/quickstart/docker-compose.quickstart.yml)
2. Set environment variable: Set `METADATA_SERVICE_AUTH_ENABLED` environment variable to `true` 
3. Redeploy DataHub GMS: Below is quickstart command to redeploy DataHub GMS 
   ```shell
   datahub docker quickstart -f docker-compose.quickstart.yml
   ```

## Plugin Permissions 
Adhere to below plugin access control to keep your plugin forward compatible.
* Plugin should read/write file to and from `plugin-home` directory only. Refer [Plugin Installation](#plugin-installation) step2 for `plugin-home` definition
* Plugin should access port 80 or 443 or port higher than 1024

All other access are forbidden for the plugin.

> In BETA version these access control are not implemented.