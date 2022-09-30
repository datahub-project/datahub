# Plugins Guide
Plugins are a way to enhance the basic DataHub functionality in a custom manner.

The listed functionalities of DataHub can be customized.

- [Authentication](#authentication)
- [Authorization](#authorization)

Refer their individual section for enhancing basic functionality of DataHub.

><span style="color:red">Note</span>: This is in <b>BETA</b> version

## Authentication 
The sample authenticator implementation can be found at [Authenticator Sample](../metadata-service/plugin/src/test/sample-test-plugins)

You need to follow below steps to implement a custom authenticator 
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
4. Refer section (Plugin Installation)[#plugin_installation] for plugin installation in DataHub environment
5. Refer section (Plugin Permissions)[#plugin_permissions] for permissions a plugin has while executing in DataHub environment


## Authorization
The sample authorizer implementation can be found at [Authorizer Sample](../metadata-auth/apache-ranger-plugin)

<TBD>

## Plugin Installation

<TBD>

## Plugin Permissions 
<TBD>
