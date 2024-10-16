# A Custom Metadata Model

[This module](./) hosts a gradle project where you can store your custom metadata model. It contains an example extension for you to follow.

### Caveats

Currently, this project only supports aspects defined in PDL to existing or newly defined entities. You cannot add new aspects to the metadata model directly through yaml configuration yet.

## Pre-Requisites

Before proceeding further, make sure you understand the DataHub Metadata Model concepts defined [here](/docs/modeling/metadata-model.md) and extending the model defined [here](/docs/modeling/extending-the-metadata-model.md). 

## Create your new aspect(s)

Follow the regular process in creating a new aspect by adding it to the [`src/main/pegasus`](./src/main/pegasus) folder. e.g. This repository has an Aspect called `customDataQualityRules` hosted in the [`DataQualityRules.pdl`](./src/main/pegasus/com/mycompany/dq/DataQualityRules.pdl) file that you can follow.
Once you've gone through this exercise, feel free to delete the sample aspects that are stored in this module.

**_Tip_**: PDL requires that the name of the file must match the name of the class that is defined in it and the package path must also match the directory path, so keep that in mind when you create your aspect pdl file.

## Add your aspect(s) to the entity registry

Add your new aspect(s) to the entity registry by editing the yaml file located under `registry/entity-registry.yaml`.
**_Note_**: The registry file must be called `entity-registry.yaml` or `entity-registry.yml` for it to be recognized.

## Understanding the entity registry

Here is a sample entity-registry file
```yaml
id: mycompany-dq-model
entities:
  - name: dataset
    aspects:
      - customDataQualityRules
```

The entity registry has a few important fields to pay attention to: 
- id: The name of your registry. This drives naming, artifact generation, so make sure you pick a unique name that will not conflict with other names you might create for other registries. 
- entities: A list of entities with aspects attached to them that you are creating additional aspects for as well as any new entities you wish to define. In this example, we are adding the aspect `customDataQualityRules` to the `dataset` entity. 

## Build your new model 

Change your directory to the metadata-models-custom folder and then run this command

```
../gradlew build
```

This will create a zip file in the build/dist folder. Then change your directory back to the main datahub folder and run 

```
./gradlew :metadata-models-custom:modelDeploy
```

This will install the zip file as a datahub plugin. It is installed at `~/.datahub/plugins/models/` and if you list the directory you should see the following path if you are following the customDataQualityRules implementation example: `~/.datahub/plugins/models/mycompany-dq-model/0.0.0-dev/`

### Build a versioned artifact
```
../gradlew -PprojVersion=0.0.1 build
```

This will deposit an artifact called `metadata-models-custom-<version>.zip` under the `build/dist` directory. 

### Deploy your versioned artifact to DataHub

```
../gradlew -PprojVersion=0.0.1 install
```

This will unpack the artifact and deposit it under `~/.datahub/plugins/models/<registry-name>/<registry-version>/`.

#### Deploying to a remote Kubernetes server

Deploying your customized jar to a remote Kubernetes server requires that you take the output zip 
(generated from `../gradlew modelArtifact` under `build/dist`) and place the unzipped contents in the volumes mount for the GMS pod on the remote server.
First you will need to push the files into a configmap using kubectl:
```
kubectl create configmap custom-model --from-file=<<path-to-file>> -n <<namespace>>
```
Then you need to set the volumes for GMS (refer to how jmx exporter configmap is added here:
https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/subcharts/datahub-gms/templates/deployment.yaml#L40)
This tells GMS that we will be pulling this configmap in. You can do this by setting `datahub-gms.extraVolumes` in `values.yaml`
which gets appended to the deployment without having to change the helm chart.

Finally you need to mount the volume into the container’s local directory by setting volumeMounts. 
Refer to how the kafka certs are mounted onto a local path here:
https://github.com/acryldata/datahub-helm/blob/master/charts/datahub/subcharts/datahub-gms/templates/deployment.yaml#L182
You can do this by setting the datahub-gms.extraVolumeMounts in `values.yaml`

at the end your values.yaml should have something like:
```
datahub-gms:
  ...
  extraVolumes:
    - name: custom-model
      configMap:
        name: custom-model ## should match configmap name above
  extraVolumeMounts:
    - name: custom-model-dir
      mountPath: /etc/plugins/models/<registry-name>/<registry-version>
```

The mountPath can be configured using `ENTITY_REGISTRY_PLUGIN_PATH` and defaults to `/etc/datahub/plugins/models`.


### Check if your model got loaded successfully 

Assuming that you are running DataHub on localhost, you can curl the config endpoint to see the model load status. 

```
curl -s http://localhost:8080/config | jq .
```

```
{
  "models": {
    "mycompany-dq-model": {
      "0.0.1": {
        "loadResult": "SUCCESS",
        "registryLocation": "/Users/username/.datahub/plugins/models/mycompany-dq-model/0.0.1",
        "failureCount": 0
      }
    }
  },
  "noCode": "true"
}
```

Alternatively, you could type in http://localhost:8080/config in your browser.

### Add some metadata with your new model 

We have included some sample scripts that you can modify to upload data corresponding to your new data model. 
The `scripts/insert_one.sh` script takes the `scripts/data/dq_rule.json` file and attaches it to the `dataset_urn` entity using the `datahub` cli. 

```console
cd scripts
./insert_one.sh
```
results in 
```console
Update succeeded with status 200
```

The `scripts/insert_custom_aspect.py` script shows you how to accomplish the same using the Python SDK. Note that we are just using a raw dictionary here to represent the `dq_rule` aspect and not a strongly-typed class.
```console
cd scripts
python3 insert_custom_aspect.py
```
results in
```console
Successfully wrote to DataHub
```

## Advanced Guide

A few things that you will likely do as you start creating new models and creating metadata that conforms to those models. 

### Deleting metadata associated with a model 

The `datahub` cli supports deleting metadata associated with a model as a customization of the `delete` command. 

e.g. `datahub delete by-registry --registry-id=mycompany-dq-model:0.0.1 --hard` will delete all data written using this registry name and version pair. 

### Evolve the metadata model

As you evolve the metadata model, you can publish new versions of the repository and deploy it into DataHub as well using the same steps outlined above. DataHub will check whether your new models are backwards compatible with the previous versioned model and decline loading models that are backwards incompatible. 

###  Custom Plugins

Adding custom aspects to DataHub's existing data model is a powerful way to extend DataHub without forking the entire repo. Often extending
just the data model is not enough and additional custom code might be required. For a few of these use cases a plugin framework was developed
to control how instances of custom aspects can be validated, mutated, and generate side effects (additional aspects).

It should be noted that validation, mutation, and generation of the *core* DataHub aspects can lead to system corruption and should be used
by advanced users only.

The `/config` endpoint documented above has been extended to return information on the instances of the various plugins as well as the classes
that were loaded for debugging purposes.

```json
{
  "mycompany-dq-model": {
    "0.0.0-dev": {
      "plugins": {
        "validatorCount": 1,
        "mutationHookCount": 1,
        "mcpSideEffectCount": 1,
        "mclSideEffectCount": 1,
        "validatorClasses": [
          "com.linkedin.metadata.aspect.plugins.validation.CustomDataQualityRulesValidator"
        ],
        "mutationHookClasses": [
          "com.linkedin.metadata.aspect.plugins.hooks.CustomDataQualityRulesMutator"
        ],
        "mcpSideEffectClasses": [
          "com.linkedin.metadata.aspect.plugins.hooks.CustomDataQualityRulesMCPSideEffect"
        ],
        "mclSideEffectClasses": [
          "com.linkedin.metadata.aspect.plugins.hooks.CustomDataQualityRulesMCLSideEffect"
        ]
      }
    }
  }
}
```

#### Custom Plugin Ecosystem Overview

The following diagram shows the overall picture of the various validators, mutators, and side effects shown within
the context of typical read/write operations within DataHub. Each component is discussed in further detail in the 
sections below.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/metadata-models-custom/custom-plugin-overview.svg"/>
</p>

In the diagram above, the circles represent Aspects (custom aspects or standard). As the Aspects progress they can be mutated/changed,
rejected, or additional aspects can be generated by side effects.

#### Custom Validators

Custom aspects might require that instances of those aspects adhere to specific conditions or rules. These conditions could vary wildly depending on the use case however they could be as simple
as a null or range check for one or more fields within the custom aspect. Additionally, a lookup can be done on other aspects in order to validate the current aspect using the `AspectRetriever`.

There are two integration points for validation. The first integration point is `on request` via the `validateProposedAspect` method where the aspect is validated independent of the previous value. This validation is performed
outside of a database transaction and can perform more intensive checks without introducing added latency within a transaction. Note that added latency from the validation check is still introduced into the request itself.

The second integration point for validation occurs within the database transaction using the `validatePreCommitAspect` and has access to the new aspect as well as the old aspect. See the included
example in [`CustomDataQualityRulesValidator.java`](src/main/java/com/linkedin/metadata/aspect/plugins/validation/CustomDataQualityRulesValidator.java).

Shown below is the interface to be implemented for a custom validator.

```java
public class CustomDataQualityRulesValidator extends AspectPayloadValidator {
    @Override
    protected Stream<AspectValidationException> validateProposedAspects(
            @Nonnull Collection<? extends BatchItem> mcpItems, @Nonnull AspectRetriever aspectRetriever) {
    }

    @Override
    protected Stream<AspectValidationException> validatePreCommitAspects(
            @Nonnull Collection<ChangeMCP> changeMCPs, AspectRetriever aspectRetriever) {
    }
}
```

In order to register this custom validator add the following to your `entity-registry.yml` file. This will activate
the validator to run on upsert operations for any entity with the custom aspect `customDataQualityRules`. Alternatively separate
validators could be written within the context of specific entities, in this case simply specify the entity name instead of `*`.

```yaml

plugins:
  aspectPayloadValidators:
    - className: 'com.linkedin.metadata.aspect.plugins.validation.CustomDataQualityRulesValidator'
      enabled: true
      supportedOperations:
        - UPSERT
      supportedEntityAspectNames:
        - entityName: '*'
          aspectName: customDataQualityRules
```

#### Custom Mutator

**Warning: This hook is for advanced users only. It is possible to corrupt data and render your system inoperable.**

Mutation hooks have two possible mutation points. The first is the `write` mutation which can change the data
being written to persistent storage. The second mutation hook is a `read` hook which can modify the data when
read from persistent storage.

Write Mutation:

In this example, we want to make sure that the field type is always lowercase regardless of the string being provided
by ingestion. The full example can be found in [`CustomDataQualityMutator.java`](src/main/java/com/linkedin/metadata/aspect/plugins/hooks/CustomDataQualityRulesMutator.java).

```java
public class CustomDataQualityRulesMutator extends MutationHook {
    @Override
    protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
            @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull AspectRetriever aspectRetriever) {
        return changeMCPS.stream()
                .map(
                        changeMCP -> {
                            boolean mutated = false;

                            if (changeMCP.getRecordTemplate() != null) {
                                DataQualityRules newDataQualityRules =
                                        new DataQualityRules(changeMCP.getRecordTemplate().data());

                                for (DataQualityRule rule : newDataQualityRules.getRules()) {
                                    // Ensure uniform lowercase
                                    if (!rule.getType().toLowerCase().equals(rule.getType())) {
                                        mutated = true;
                                        rule.setType(rule.getType().toLowerCase());
                                    }
                                }
                            }

                            return mutated ? changeMCP : null;
                        })
                .filter(Objects::nonNull)
                .map(changeMCP -> Pair.of(changeMCP, true));
    }
}
```

```yaml
plugins:
  mutationHooks:
    - className: 'com.linkedin.metadata.aspect.plugins.hooks.CustomDataQualityRulesMutator'
      enabled: true
      supportedOperations:
        - UPSERT
      supportedEntityAspectNames:
        - entityName: '*'
          aspectName: customDataQualityRules
```

Read Mutation:

A read mutator would implement the following interface and the following example is a read mutation which hides soft 
deleted structured properties from being returned on entities. 

```java
public class StructuredPropertiesSoftDelete extends MutationHook {
    @Override
    protected Stream<Pair<ReadItem, Boolean>> readMutation(
            @Nonnull Collection<ReadItem> items, @Nonnull AspectRetriever aspectRetriever) {
        Map<Urn, StructuredProperties> entityStructuredPropertiesMap =
                items.stream()
                        .filter(i -> i.getRecordTemplate() != null)
                        .map(i -> Pair.of(i.getUrn(), i.getAspect(StructuredProperties.class)))
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        // Apply filter
        Map<Urn, Boolean> mutatedEntityStructuredPropertiesMap =
                StructuredPropertyUtils.filterSoftDelete(entityStructuredPropertiesMap, aspectRetriever);

        return items.stream()
                .map(i -> Pair.of(i, mutatedEntityStructuredPropertiesMap.getOrDefault(i.getUrn(), false)));
    }
}
```

Note that the `supportedOperations` is left empty since those operation types only include change types like `UPSERT` or `DELETE`

```yaml
plugins:
  mutationHooks:
    - className: 'com.linkedin.metadata.aspect.plugins.hooks.CustomDataQualityRulesMutator'
      enabled: true
      supportedEntityAspectNames:
        - entityName: '*'
          aspectName: customDataQualityRules
```

#### MetadataChangeProposal (MCP) Side Effects

**Warning: This hook is for advanced users only. It is possible to corrupt data and render your system inoperable.**

MCP Side Effects allow for the creation of new aspects based on an input aspect.

Notes:
* MCPs will write aspects to the primary data store (SQL for example) as well as the search indices.
* Side effects in general must include a dependency on the `metadata-io` module since it deals with lower level storage primitives.

The full example can be found in [`CustomDataQualityRulesMCPSideEffect.java`](src/main/java/com/linkedin/metadata/aspect/plugins/hooks/CustomDataQualityRulesMCPSideEffect.java).

```java
public class CustomDataQualityRulesMCPSideEffect extends MCPSideEffect {
    @Override
    protected Stream<ChangeMCP> applyMCPSideEffect(
            Collection<ChangeMCP> changeMCPS, @Nonnull AspectRetriever aspectRetriever) {
        // Mirror aspects to another URN in SQL & Search
        return changeMCPS.stream()
                .map(
                        changeMCP -> {
                            Urn mirror =
                                    UrnUtils.getUrn(changeMCP.getUrn().toString().replace(",PROD)", ",DEV)"));
                            return ChangeItemImpl.builder()
                                    .urn(mirror)
                                    .aspectName(changeMCP.getAspectName())
                                    .recordTemplate(changeMCP.getRecordTemplate())
                                    .auditStamp(changeMCP.getAuditStamp())
                                    .systemMetadata(changeMCP.getSystemMetadata())
                                    .build(aspectRetriever);
                        });
    }
}
```

```yaml
plugins:
  mcpSideEffects:
    - className: 'com.linkedin.metadata.aspect.plugins.hooks.CustomDataQualityRulesMCPSideEffect'
      enabled: true
      supportedOperations:
        - UPSERT
      supportedEntityAspectNames:
        - entityName: '*'
          aspectName: customDataQualityRules
```

#### MetadataChangeLog (MCL) Side Effects

**Warning: This hook is for advanced users only. It is possible to corrupt data and render your system inoperable.**

MCL Side Effects allow for the creation of new aspects based on an input aspect. In this example, we are generating a timeseries aspect to represent an event. When a DataQualityRule is created
or modified we'll record the actor, event type, and timestamp in a timeseries aspect index.

Notes:
* MCLs are only persisted to the search indices which allows for adding to the search documents only.
* Dependency on the `metadata-io` module since it deals with lower level storage primitives.

The full example can be found in [`CustomDataQualityRulesMCLSideEffect.java`](src/main/java/com/linkedin/metadata/aspect/plugins/hooks/CustomDataQualityRulesMCLSideEffect.java).

```java
public class CustomDataQualityRulesMCLSideEffect extends MCLSideEffect {
    @Override
    protected Stream<MCLItem> applyMCLSideEffect(
            @Nonnull Collection<MCLItem> mclItems, @Nonnull AspectRetriever aspectRetriever) {
        return mclItems.stream()
                .map(
                        item -> {
                            // Generate Timeseries event aspect based on non-Timeseries aspect
                            MetadataChangeLog originMCP = item.getMetadataChangeLog();

                            return buildEvent(originMCP)
                                    .map(
                                            event -> {
                                                try {
                                                    MetadataChangeLog eventMCP = originMCP.clone();
                                                    eventMCP.setAspect(GenericRecordUtils.serializeAspect(event));
                                                    eventMCP.setAspectName("customDataQualityRuleEvent");
                                                    return eventMCP;
                                                } catch (CloneNotSupportedException e) {
                                                    throw new RuntimeException(e);
                                                }
                                            })
                                    .map(
                                            eventMCP ->
                                                    MCLItemImpl.builder().metadataChangeLog(eventMCP).build(aspectRetriever));
                        })
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private Optional<DataQualityRuleEvent> buildEvent(MetadataChangeLog originMCP) {
        if (originMCP.getAspect() != null) {
            DataQualityRuleEvent event = new DataQualityRuleEvent();
            if (event.getActor() != null) {
                event.setActor(event.getActor());
            }
            event.setEventTimestamp(originMCP.getSystemMetadata().getLastObserved());
            event.setTimestampMillis(originMCP.getSystemMetadata().getLastObserved());
            if (originMCP.getPreviousAspectValue() == null) {
                event.setEventType("RuleCreated");
            } else {
                event.setEventType("RuleUpdated");
            }
            event.setAffectedDataset(originMCP.getEntityUrn());

            return Optional.of(event);
        }
        return Optional.empty();
    }
}
```

```yaml
plugins:
  mclSideEffects:
    - className: 'com.linkedin.metadata.aspect.plugins.hooks.CustomDataQualityRulesMCLSideEffect'
      enabled: true
      supportedOperations:
        - UPSERT
      supportedEntityAspectNames:
        - entityName: 'dataset'
          aspectName: customDataQualityRules
```

#### Spring Support

Validators, mutators, and side-effects can also utilize Spring to inject dependencies and autoconfigure them. While Spring is
not required, it is possible to use Spring to both inject autoconfiguration and the plugins themselves. An example Spring-enabled
validator has been included in the package `com.linkedin.metadata.aspect.plugins.spring.validation`. The plugin
class loader and Spring context is isolated so conflicts between DataHub and custom classes are avoided.

The configuration of a Spring enabled plugin looks like the following, note the addition of `spring.enabled: true` below.
A list of packages to scan for Spring configuration and components should also be provided which should include
your custom classes with Spring annotations per the `packageScan` below.

```yaml
plugins:
  aspectPayloadValidators:
    - className: 'com.linkedin.metadata.aspect.plugins.spring.validation.CustomDataQualityRulesValidator'
      packageScan:
        - com.linkedin.metadata.aspect.plugins.spring.validation
      enabled: true
      supportedOperations:
        - UPSERT
      supportedEntityAspectNames:
        - entityName: 'dataset'
          aspectName: customDataQualityRules
      spring:
        enabled: true
```

In the Spring example, a configuration component called `CustomDataQualityRulesConfig` provides a string `Spring injection works!` demonstrating
injection of a bean into a function which is called by Spring after constructing the custom validator plugin.

```java
@Configuration
public class CustomDataQualityRulesConfig {
    @Bean("myCustomMessage")
    public String myCustomMessage() {
        return "Spring injection works!";
    }
}
```

```java
@Component
@Import(CustomDataQualityRulesConfig.class)
public class CustomDataQualityRulesValidator extends AspectPayloadValidator {
    @Autowired
    @Qualifier("myCustomMessage")
    private String myCustomMessage;

    @PostConstruct
    public void message() {
        System.out.println(myCustomMessage);
    }
    
    // ...
}
```

Example Log:

```
INFO  c.l.m.m.r.PluginEntityRegistryLoader:187 - com.linkedin.metadata.models.registry.PluginEntityRegistryLoader@144e466d: Registry mycompany-dq-model:0.0.0-dev discovered. Loading...
INFO  c.l.m.m.registry.PatchEntityRegistry:143 - Loading custom config entity file: /etc/datahub/plugins/models/mycompany-dq-model/0.0.0-dev/entity-registry.yaml, dir: /etc/datahub/plugins/models/mycompany-dq-model/0.0.0-dev
INFO  c.l.m.m.registry.PatchEntityRegistry:143 - Loading custom config entity file: /etc/datahub/plugins/models/mycompany-dq-model/0.0.0-dev/entity-registry.yaml, dir: /etc/datahub/plugins/models/mycompany-dq-model/0.0.0-dev
Spring injection works!
```


## The Future

Hopefully this repository shows you how easily you can extend and customize DataHub's metadata model!

We will be continuing to make the experience less reliant on core changes to DataHub and reducing the need to fork the main repository.

