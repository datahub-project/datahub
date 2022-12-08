# Plugins Guide
Plugins are way to enhance the basic DataHub functionality in a custom manner.

Currently, DataHub formally supports 2 types of plugins:

- [Authentication](#authentication)
- [Authorization](#authorization)


## Authentication

>**Note:** This is in <b>BETA</b> version

> It is recommend that you do not do this unless you really know what you are doing

Custom authentication plugin makes it possible to authenticate DataHub users against any Identity Management System.
Choose your Identity Management System and write custom authentication plugin as per detail mentioned in this section.


> Currently, custom authenticators cannot be used to authenticate users of DataHub's web UI. This is because the DataHub web app expects the presence of 2 special cookies PLAY_SESSION and actor which are explicitly set by the server when a login action is performed.
Instead, custom authenticators are useful for authenticating API requests to DataHub's backend (GMS), and can stand in addition to the default Authentication performed by DataHub, which is based on DataHub-minted access tokens.

The sample authenticator implementation can be found at [Authenticator Sample](../metadata-service/plugin/src/test/sample-test-plugins)

### Implementing an Authentication Plugin
1. Add _datahub-auth-api_ as implementation dependency: Maven coordinates of _datahub-auth-api_ can be found at [Maven](https://mvnrepository.com/artifact/io.acryl/datahub-auth-api)

   Example of gradle dependency is given below.

   ```groovy
    dependencies {
        implementation 'io.acryl:datahub-auth-api:0.8.45'
    }
   ```

2. Implement the Authenticator interface: Refer [Authenticator Sample](../metadata-service/plugin/src/test/sample-test-plugins)

   <details>
      <summary>Sample class which implements the Authenticator interface</summary>

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
   </details>

3. Use `getResourceAsStream` to read files: If your plugin read any configuration file like properties or YAML or JSON or xml then use `this.getClass().getClassLoader().getResourceAsStream("<file-name>")` to read that file from DataHub GMS plugin's class-path. For DataHub GMS resource look-up behavior please refer [Plugin Installation](#plugin-installation) section. Sample code of `getResourceAsStream` is available in sample Authenticator plugin [TestAuthenticator.java](../metadata-service/plugin/src/test/sample-test-plugins/src/main/java/com/datahub/plugins/test/TestAuthenticator.java).


4. Bundle your Jar: Use `com.github.johnrengelman.shadow` gradle plugin to create an uber jar. 

   To see an example of building an uber jar, check out the `build.gradle` file for the apache-ranger-plugin file of [Apache Ranger Plugin](https://github.com/acryldata/datahub-ranger-auth-plugin/tree/main/apache-ranger-plugin) for reference. 

   Exclude datahub plugin dependency and signature classes as shown in below `shadowJar` task.

   ```groovy
     apply plugin: 'com.github.johnrengelman.shadow';
     shadowJar {
         // Exclude com.datahub.plugins package and files related to jar signature   
         exclude "com/linkedin/common/", "com/datahub/", "META-INF/*.RSA", "META-INF/*.SF","META-INF/*.DSA"
     }
   ```
5. Refer section [Plugin Installation](#plugin-installation) for plugin installation in DataHub environment

## Enable GMS Authentication
By default, authentication is disabled in DataHub GMS.

Follow below steps to enable GMS authentication
1. Download docker-compose.quickstart.yml: Download docker compose file [docker-compose.quickstart.yml](../docker/quickstart/docker-compose.quickstart.yml)

2. Set environment variable: Set `METADATA_SERVICE_AUTH_ENABLED` environment variable to `true`

3. Redeploy DataHub GMS: Below is quickstart command to redeploy DataHub GMS

   ```shell
   datahub docker quickstart -f docker-compose.quickstart.yml
   ```

## Authorization

>**Note:** This is in <b>BETA</b> version

> It is recommend that you do not do this unless you really know what you are doing

Custom authorization plugin makes it possible to authorize DataHub users against any Access Management System.
Choose your Access Management System and write custom authorization plugin as per detail mentioned in this section.


The sample authorizer implementation can be found at [Authorizer Sample](https://github.com/acryldata/datahub-ranger-auth-plugin/tree/main/apache-ranger-plugin)

### Implementing an Authorization Plugin

1. Add _datahub-auth-api_ as implementation dependency: Maven coordinates of _datahub-auth-api_ can be found at [Maven](https://mvnrepository.com/artifact/io.acryl/datahub-auth-api)

2. Implement the Authorizer interface: [Authorizer Sample](https://github.com/acryldata/datahub-ranger-auth-plugin/tree/main/apache-ranger-plugin)

   <details>
      <summary>Sample class which implements the Authorization interface </summary>
   
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
   
   </details>

3. Use `getResourceAsStream` to read files: If your plugin read any configuration file like properties or YAML or JSON or xml then use `this.getClass().getClassLoader().getResourceAsStream("<file-name>")` to read that file from DataHub GMS plugin's class-path. For DataHub GMS resource look-up behavior please refer [Plugin Installation](#plugin-installation) section. Sample code of `getResourceAsStream` is available in sample Authenticator plugin [TestAuthenticator.java](../metadata-service/plugin/src/test/sample-test-plugins/src/main/java/com/datahub/plugins/test/TestAuthenticator.java).

4. Bundle your Jar: Use `com.github.johnrengelman.shadow` gradle plugin to create an uber jar. 

   To see an example of building an uber jar, check out the `build.gradle` file for the apache-ranger-plugin file of [Apache Ranger Plugin](https://github.com/acryldata/datahub-ranger-auth-plugin/tree/main/apache-ranger-plugin) for reference.

   Exclude datahub plugin dependency and signature classes as shown in below `shadowJar` task.

   ```groovy
     apply plugin: 'com.github.johnrengelman.shadow';
     shadowJar {
         // Exclude com.datahub.plugins package and files related to jar signature   
         exclude "com/linkedin/common/", "com/datahub/", "META-INF/*.RSA", "META-INF/*.SF","META-INF/*.DSA"
     }
   ```
   
5. Install the Plugin: Refer to the section (Plugin Installation)[#plugin_installation] for plugin installation in DataHub environment

## Plugin Installation
DataHub's GMS Service searches for the plugins in container's local directory at location `/etc/datahub/plugins/auth/`. This location will be referred as `plugin-base-directory` hereafter.

For docker, we set docker-compose to mount `${HOME}/.datahub` directory to `/etc/datahub` directory within the GMS containers.

### Docker
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
4. Update plugin configuration file: Add below entry in `config.yml` file, the plugin can take any arbitrary configuration under the "configs" block. in our example, there is username and password

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
5. Restart datahub-gms container: 

   On startup DataHub GMS service performs below steps 
      1. Load `config.yml`
      2. Prepare list of plugin where `enabled` is set to `true`
      3. Look for directory equivalent to plugin `name` in `plugin-base-directory`. In this case it is `/etc/datahub/plugins/auth/apache-ranger-authorizer/`, this directory will become `plugin-home` 
      4. Look for `params.jarFileName` attribute otherwise look for jar having name as &lt;plugin-name&gt;.jar. In this case  it is `/etc/datahub/plugins/auth/apache-ranger-authorizer/apache-ranger-authorizer.jar`
      5. Load class given in plugin `params.className` attribute from the jar, here load class `com.abc.RangerAuthorizer` from `apache-ranger-authorizer.jar`
      6. Call `init` method of plugin

   <br/>On method call of `getResourceAsStream` DataHub GMS service looks for the resource in below order.
      1. Look for the requested resource in plugin-jar file. if found then return the resource as InputStream.
      2. Look for the requested resource in `plugin-home` directory. if found then return the resource as InputStream.
      3. Look for the requested resource in application class-loader. if found then return the resource as InputStream.
      4. Return `null` as requested resource is not found.

By default, authentication is disabled in DataHub GMS, Please follow section [Enable GMS Authentication](#enable-gms-authentication) to enable authentication.

### Kubernetes
Helm support is coming soon.

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


## Plugin Permissions 
Adhere to below plugin access control to keep your plugin forward compatible.
* Plugin should read/write file to and from `plugin-home` directory only. Refer [Plugin Installation](#plugin-installation) step2 for `plugin-home` definition
* Plugin should access port 80 or 443 or port higher than 1024

All other access are forbidden for the plugin.

> Disclaimer: In BETA version your plugin can access any port and can read/write to any location on file system, however you should implement the plugin as per above access permission to keep your plugin compatible with upcoming release of DataHub.

## Migration Of Plugins From application.yml
If you have any custom Authentication or Authorization plugin define in `authorization` or `authentication` section of  [application.yml](../metadata-service/factories/src/main/resources/application.yml) then migrate them as per below steps.

1. Implement Plugin: For Authentication Plugin follow steps of [Implementing an Authentication Plugin](#implementing-an-authentication-plugin) and for Authorization Plugin follow steps of [Implementing an Authorization Plugin](#implementing-an-authorization-plugin)
2. Install Plugin: Install the plugins as per steps mentioned in [Plugin Installation](#plugin-installation). Here you need to map the configuration from [application.yml](../metadata-service/factories/src/main/resources/application.yml) to configuration in `config.yml`. This mapping from `application.yml` to `config.yml` is described below 

   **Mapping for Authenticators**

   a. In `config.yml` set `plugins[].type` to `authenticator`

   b. `authentication.authenticators[].type` is mapped to `plugins[].params.className`
   
   c. `authentication.authenticators[].configs` is mapped to `plugins[].params.configs`
   
   Example Authenticator Plugin configuration in `config.yml`

   ```yaml
   plugins:
      - name: "apache-ranger-authenticator"
         type: "authenticator"
         enabled: "true"
         params:
         className: "com.abc.RangerAuthenticator"
         configs:
            username: "foo"
            password: "fake"
   
   ```

   
   **Mapping for Authorizer**

   a. In `config.yml` set `plugins[].type` to `authorizer`

   b. `authorization.authorizers[].type` is mapped to `plugins[].params.className`
   
   c. `authorization.authorizers[].configs` is mapped to `plugins[].params.configs`

   Example Authorizer Plugin configuration in `config.yml`

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
3. Move any other configurations files of your plugin to `plugin_home` directory. The detail about `plugin_home` is mentioned in [Plugin Installation](#plugin-installation) section.