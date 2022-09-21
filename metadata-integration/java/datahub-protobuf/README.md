# Protobuf Schemas

The `datahub-protobuf` module is designed to be used with the Java Emitter, the input is a compiled protobuf binary `*.protoc` files and optionally the corresponding `*.proto` source code. In addition, you can supply the root message in cases where a single protobuf source file includes multiple non-nested messages.

## Supported Features

The following protobuf features are supported and are translated into descriptions, tags, properties and terms on a dataset.

    * C++/C style code comments on Messages and Fields
    * Nested Types
    * Scalar Values
    * Well Known Type Wrappers (i.e. DoubleValue, FloatValue, StringValue)
    * Enumerations
    * Oneof
    * Maps
    * Extensions
    * Web links
    * Parsing of GitHub team names and slack channel references

## Usage

### Protobuf Compile Options

In order to support parsing comments the following option flags should be used during `protoc` compilation.

    protoc --include_imports --include_source_info --descriptor_set_out=MyProto.protoc MyProto.proto

### Code Example

Given an input stream of the `protoc` binary and the emitter the minimal code is shown below.

```java
import com.linkedin.common.FabricType;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import datahub.client.rest.RestEmitter;
import datahub.protobuf.ProtobufDataset;

RestEmitter emitter;
InputStream protocInputStream;

AuditStamp auditStamp = new AuditStamp()
    .setTime(System.currentTimeMillis())
    .setActor(new CorpuserUrn("datahub"));

ProtobufDataset dataset = ProtobufDataset.builder()
    .setDataPlatformUrn(new DataPlatformUrn("kafka"))
    .setProtocIn(protocInputStream)
    .setAuditStamp(auditStamp)
    .setFabricType(FabricType.DEV)
    .build();

dataset.getAllMetadataChangeProposals().flatMap(Collection::stream).forEach(mcpw -> emitter.emit(mcpw, null).get());
```

Additionally, the raw protobuf source can be included as well as information to allow parsing of additional references to GitHub and Slack in the source code comments.

```java
ProtobufDataset dataset = ProtobufDataset.builder()
    .setDataPlatformUrn(new DataPlatformUrn("kafka"))
    .setSchema(" my raw protobuf schema ")
    .setProtocIn(protocInputStream)
    .setAuditStamp(auditStamp)
    .setFabricType(FabricType.DEV)
    .setGithubOrganization("myOrg")
    .setSlackTeamId("SLACK123")
    .build();
```

### Protobuf Extensions

In order to extract even more metadata from the protobuf schema we can extend the FieldOptions and MessageOptions to be able to annotate Messages and Fields with arbitrary information. This information can then be emitted as DataHub primary key information, tags, glossary terms or properties on the dataset.

An annotated protobuf schema would look like the following, except for the `is_primary_key` all annotations are configurable for individual needs.

*Note*: Extending FieldOptions and MessageOptions does not change the messages themselves. The metadata is not included in messages being sent over the wire.

```protobuf
syntax = "proto3";
import "meta.proto";

message Department {
    int32 id = 1 [(meta.fld.is_primary_key) = true];
    string name = 2;
}

message Person {
    option(meta.msg.type) = ENTITY;
    option(meta.msg.classification_enum) = HighlyConfidential;
    option(meta.msg.team) = "TeamB";
    option(meta.msg.bool_feature) = true;
    option(meta.msg.alert_channel) = "#alerts";

    string name = 1 [(meta.fld.classification) = "Classification.HighlyConfidential"];

    int32 id = 2
    [(meta.fld.is_primary_key) = true];

    string email = 3
    [(meta.fld.classification_enum) = Confidential];
    
    Department dept = 4;
    
    string test_coverage = 5
    [(meta.fld.product_type_bool) = true, (meta.fld.product_type) = "my type", (meta.fld.product_type_enum) = EVENT];
}
```

#### meta.proto 

In order to use the annotations above, create a proto file called `meta.proto`. Feel free to customize the kinds of metadata and how it is emitted to DataHub for your use cases.

```protobuf
syntax = "proto3";
package meta;

import "google/protobuf/descriptor.proto";

/*
   This is assigned to metadata fields. It describes how the metadata field should be represented
   in DataHub. This enum must be used in the `meta` package. Multiple can be used for the same
   metadata annotation. This allows a single piece of information to be captured in DataHub
   as a property, tag and/or term.

   Tags can be strings, enums, or booleans
   Terms can be strings or enums
   Properties should be strings

*/
enum DataHubMetadataType {
  PROPERTY        = 0; // Datahub Custom Property
  TAG             = 1; // Datahub Tag
  TAG_LIST        = 2; // Datahub Tags from comma delimited string
  TERM            = 3; // Datahub Term
  OWNER           = 4; // Datahub Owner
  DOMAIN          = 5; // Datahub Domain
  DEPRECATION     = 6; // Datahub Deprecation
}

/*
   Example below: The following is not required for annotation processing. This is an example
   of creating an annotation using an enum.
 */

enum MetaEnumExample {
  UNKNOWN = 0;
  ENTITY = 1;
  EVENT = 2;
}

// Assuming Glossary Term defined from bootstrap example
enum Classification {
  HighlyConfidential = 0;
  Confidential = 1;
  Sensitive = 2;
}
```

#### FieldOptions

Define possible annotations on fields and how they are exported to DataHub.

```protobuf

message fld {
  extend google.protobuf.FieldOptions {
    // Required: Mark option field with how to export to DataHub in one or more places.
    repeated meta.DataHubMetadataType type = 6000;

    /*
       Examples below: The following is not required for annotation processing.
     */

    // Set true if the field is a primary key. This works for any boolean with `primary_key` in it.
    bool is_primary_key = 6010;

    // Extract classification field option as a Term, either works
    string classification = 6001 [(meta.fld.type) = TERM];
    meta.Classification classification_enum = 6002 [(meta.fld.type) = TERM];

    // Expose this option as a tag on the field.
    string product_type = 70004 [(meta.fld.type) = TAG];
    bool product_type_bool = 70005 [(meta.fld.type) = TAG];
    meta.MetaEnumExample product_type_enum = 70006 [(meta.fld.type) = TAG];
  }
}
```

#### MessageOptions

Define possible annotations on messages and how they are exported to DataHub.

```protobuf

message msg {
  extend google.protobuf.MessageOptions {
    /*
       Examples below: The following is not required for annotation processing.
     */

    // Place the classification term at the Message/Dataset level, either string or enum is supported
    string classification = 4000 [(meta.fld.type) = TERM, (meta.fld.type) = PROPERTY];
    meta.Classification classification_enum = 4001 [(meta.fld.type) = TERM, (meta.fld.type) = PROPERTY];

    // Attach these Message/Dataset options as a tag and property.
    string product = 5001 [(meta.fld.type) = TAG, (meta.fld.type) = PROPERTY];
    string project = 5002 [(meta.fld.type) = TAG, (meta.fld.type) = PROPERTY];
    string team = 5003 [(meta.fld.type) = OWNER, (meta.fld.type) = PROPERTY];

    string domain = 60003 [(meta.fld.type) = DOMAIN, (meta.fld.type) = PROPERTY];
    meta.MetaEnumExample type = 60004 [(meta.fld.type) = TAG, (meta.fld.type) = PROPERTY];
    bool bool_feature = 60005 [(meta.fld.type) = TAG];
    string alert_channel = 60007 [(meta.fld.type) = PROPERTY];

    repeated string deprecation_note = 60008 [(meta.fld.type) = DEPRECATION, (meta.fld.type) = PROPERTY];
    uint64 deprecation_time          = 60009 [(meta.fld.type) = DEPRECATION, (meta.fld.type) = PROPERTY];
  }
}
```

#### DataHubMetadataType

| DataHubMetadataType | String    | Bool | Enum | Repeated  | Uint64   |
|---------------------|-----------|------|------|-----------|----------|
| PROPERTY            | X         | X    | X    | X         |          |
| TAG                 | X         | X    | X    |           |          |
| TAG_LIST            | X         |      |      |           |          |
| TERM                | X         |      | X    |           |          |
| OWNER               | X         |      |      | X         |          |
| DOMAIN              | X         |      |      | X         |          |
| DEPRECATION         | X (notes) |      |      | X (notes) | X (time) |

##### PROPERTY

Custom properties can be captured as key/value pairs where the protobuf option name is the key and the option value is the option's value.

For example, generating a custom property with key `prop1` and value `value1`.

```protobuf
   message msg {
     extend google.protobuf.MessageOptions {
       string prop1 = 5000 [(meta.fld.type) = PROPERTY];
    }
  }
  
  message Message {
    option(meta.msg.prop1) = "value1";
  }
```

Booleans are converted to a value of either `true` or `false`.

```protobuf
   message msg {
     extend google.protobuf.MessageOptions {
       bool prop1 = 5000 [(meta.fld.type) = PROPERTY];
    }
  }
  
  message Message {
    option(meta.msg.prop1) = true;
  }
```

Enum values are similarly converted to their string representation.

```protobuf
   enum MetaEnumExample {
     UNKNOWN = 0;
     ENTITY = 1;
     EVENT = 2;
   }

   message msg {
     extend google.protobuf.MessageOptions {
       MetaEnumExample prop1 = 5000 [(meta.fld.type) = PROPERTY];
    }
  }
  
  message Message {
    option(meta.msg.prop1) = ENTITY;
  }
```

Repeated values will be collected and the value will be stored as a serialized json array. The following example would result in the value of `["a","b","c"]`.

```protobuf
   message msg {
     extend google.protobuf.MessageOptions {
       repeated string prop1 = 5000 [(meta.fld.type) = PROPERTY];
    }
  }
  
  message Message {
    option(meta.msg.prop1) = "a";
    option(meta.msg.prop1) = "b";
    option(meta.msg.prop1) = "c";
  }
```

##### TAG & TAG_LIST

The tag list assumes a string that contains the comma delimited values of the tags. In the example below, tags would be added as `a`, `b`, `c`.

```protobuf
   message msg {
     extend google.protobuf.MessageOptions {
       string tags = 5000 [(meta.fld.type) = TAG_LIST];
    }
  }
  
  message Message {
    option(meta.msg.tags) = "a, b, c";
  }
```

Tags could also be represented as separate boolean options. Only the `true` options result in tags. In this example, a single tag of `tagA` would be added to the dataset.

```protobuf
   message msg {
     extend google.protobuf.MessageOptions {
       bool tagA = 5000 [(meta.fld.type) = TAG];
       bool tagB = 5001 [(meta.fld.type) = TAG];
    }
  }
  
  message Message {
    option(meta.msg.tagA) = true;
    option(meta.msg.tagB) = false;
  }
```

Alternatively, tags can be separated into different fields with the option name as a dot delimited prefix. The following would produce two tags with values of `tagA.a` and `tagB.a`.

```protobuf
   message msg {
     extend google.protobuf.MessageOptions {
       string tagA = 5000 [(meta.fld.type) = TAG];
       string tagB = 5001 [(meta.fld.type) = TAG];
    }
  }
  
  message Message {
    option(meta.msg.tagA) = "a";
    option(meta.msg.tagB) = "a";
  }
```

The dot delimited prefix also works with enum types where the prefix is the enum type name. In this example two tags are created, `MetaEnumExample.ENTITY`.

```protobuf
  enum MetaEnumExample {
    UNKNOWN = 0;
    ENTITY = 1;
    EVENT = 2;
  }

   message msg {
     extend google.protobuf.MessageOptions {
       MetaEnumExample tag = 5000 [(meta.fld.type) = TAG];
    }
  }
  
  message Message {
    option(meta.msg.tag) = ENTITY;
  }
```

In addition, tags can be added to fields as well as messages. The following is a consolidated example for all the possible tag options on fields.

```protobuf
  enum MetaEnumExample {
    UNKNOWN = 0;
    ENTITY = 1;
    EVENT = 2;
  }

   message fld {
     extend google.protobuf.FieldOptions {
       string tags             = 6000 [(meta.fld.type) = TAG_LIST];
       string tagString        = 6001 [(meta.fld.type) = TAG];
       bool tagBool            = 6002 [(meta.fld.type) = TAG];
       MetaEnumExample tagEnum = 6003 [(meta.fld.type) = TAG];
    }
  }
  
  message Message {
    uint32 my_field = 1
        [(meta.fld.tags) = "a, b, c",
         (meta.fld.tagString) = "myTag",
         (meta.fld.tagBool) = true,
         (meta.fld.tagEnum) = ENTITY];
  }
```

##### TERM

Terms are specified by either a fully qualified string value or an enum where the enum type's name is the first element in the fully qualified term name.

The following example shows both methods, either of which would result in the term `Classification.HighlyConfidential` being applied.

```protobuf
   enum Classification {
     HighlyConfidential = 0;
     Confidential = 1;
     Sensitive = 2;
   }

   message msg {
     extend google.protobuf.MessageOptions {
       Classification term = 5000 [(meta.fld.type) = TERM];
       string class = 5001 [(meta.fld.type) = TERM];
    }
  }
  
  message Message {
    option(meta.msg.term) = HighlyConfidential;
    option(meta.msg.class) = "Classification.HighlyConfidential";
  }
```

The following is a consolidated example for the possible field level term options.

```protobuf
   enum Classification {
    HighlyConfidential = 0;
    Confidential = 1;
    Sensitive = 2;
  }

   message fld {
     extend google.protobuf.FieldOptions {
       Classification term = 5000 [(meta.fld.type) = TERM];
       string class = 5001 [(meta.fld.type) = TERM];
    }
  }
  
  message Message {
    uint32 my_field = 1
        [(meta.fld.term) = HighlyConfidential,
         (meta.fld.class) = "Classification.HighlyConfidential"];
  }
```

##### OWNER

One or more owners can be specified and can be any combination of `corpUser` and `corpGroup` entities. The default entity type is `corpGroup`. By default, the ownership type is set to `technical_owner`, see the second example for setting the ownership type.

The following example assigns the ownership to a group of `myGroup` and a user called `myName`.

```protobuf
   message msg {
     extend google.protobuf.MessageOptions {
       repeated string owner = 5000 [(meta.fld.type) = OWNER];
    }
  }
  
  message Message {
    option(meta.msg.owner) = "corpUser:myName";
    option(meta.msg.owner) = "myGroup";
  }
```

In this example, the option name determines the ownership type. User `myName` is assigned as the Technical Owner and `myGroup` as the Data Steward.

```protobuf
   message msg {
     extend google.protobuf.MessageOptions {
       repeated string technical_owner = 5000 [(meta.fld.type) = OWNER];
       repeated string data_steward = 5001 [(meta.fld.type) = OWNER];
    }
  }
  
  message Message {
    option(meta.msg.technical_owner) = "corpUser:myName";
    option(meta.msg.data_steward) = "myGroup";
  }
```

##### DOMAIN

Set the domain id for the dataset. The domain should exist already. Note that the *id* of the domain is the value. If not specified during domain creation it is likely a random string.

```protobuf
   message msg {
     extend google.protobuf.MessageOptions {
       string domain = 5000 [(meta.fld.type) = DOMAIN];
    }
  }
  
  message Message {
    option(meta.msg.domain) = "engineering";
  }
```

##### DEPRECATION

Deprecation of fields and messages are natively supported by protobuf options.
The standard "Deprecation" aspect is used for a dataset generated from a protobuf `message`.
Field deprecation adds a tag with the following urn `urn:li:tag:deprecated` (red, #FF000).

```protobuf
   message msg {
     extend google.protobuf.MessageOptions {
       repeated string deprecation_note = 5620 [(meta.fld.type) = DEPRECATION];
       uint64 deprecation_time          = 5621 [(meta.fld.type) = DEPRECATION];
    }
  }
  
  message Message {
    option deprecated = true;
    option (meta.msg.deprecation_note) = "Deprecated for this other message.";
    option (meta.msg.deprecation_note) = "Drop in replacement.";
    option (meta.msg.deprecation_time) = 1649689387;
  }
```

The field deprecation tag works without definition in `meta.proto` using the native protobuf option.

```protobuf
message Message {
  uint32 my_field = 1 [deprecated = true];
}
```

## Installation

Follow the specific instructions for your build system to declare a dependency on the appropriate version of the package.

**_Note_**: Check the [Maven repository](https://mvnrepository.com/artifact/io.acryl/datahub-protobuf) for the latest version of the package before following the instructions below.

### Gradle
Add the following to your build.gradle.
```gradle
implementation 'io.acryl:datahub-protobuf:__version__'
```
### Maven
Add the following to your `pom.xml`.
```xml
<!-- https://mvnrepository.com/artifact/io.acryl/datahub-protobuf -->
<dependency>
    <groupId>io.acryl</groupId>
    <artifactId>datahub-protobuf</artifactId>
    <!-- replace __version__ with the latest version number -->
    <version>__version__</version>
</dependency>
```

## Example Application (embedded)

An example application **Proto2DataHub** is included as part of this project. 
You can also set up a standalone project that works with the `protobuf-gradle-plugin`, see the standalone [example project](../datahub-protobuf-example) as an example of such a project.

### Usage

#### Standalone Application: Proto2DataHub

```
shell
java -jar build/libs/datahub-protobuf-0.8.45-SNAPSHOT.jar --help
usage: Proto2DataHub
    --datahub_api <arg>     [Optional] The API endpoint for DataHub GMS.
                            (defaults to https://localhost:8080)
    --datahub_token <arg>   [Optional] The authentication token for
                            DataHub API access. (defaults to empty)
    --datahub_user <arg>    [Optional] The datahub user to attribute this
                            ingestion to. (defaults to ..)
    --descriptor <arg>      [Required] The generated protobuf descriptor
                            file. Typically a single .dsc file for the
                            repo or a .protoc file (1:1 with each src
                            file)
    --directory <arg>       [Optional if using --file] The root directory
                            containing protobuf source files.
    --env <arg>             [Optional] The environment to attach all
                            entities to. Typically, DEV, PROD etc.
                            (defaults to DEV)
    --exclude <arg>         [Optional] Exclude patterns to avoid
                            processing all source files, separated by ,.
                            Typically used with --directory option.
                            Follows glob patterns: e.g. --exclude
                            "build/**,generated/**" will exclude all files
                            in the build and generated directories under
                            the rootDirectory given by the --directory
                            option
    --file <arg>            [Optional if using --directory] The protobuf
                            source file. Typically a .proto file.
    --filename <arg>        [Required if using transport file] Filename to
                            write output to.
    --github_org <arg>      [Optional] The GitHub organization that this
                            schema repository belongs to. We will
                            translate comments in your protoc files like
                            @datahub-project/data-team to GitHub team urls
                            like:
                            https://github.com/orgs/datahub-project/teams/
                            data-team
    --help                  Print this help message
    --platform <arg>        [Optional] The data platform to produce
                            schemas for. e.g. kafka, snowflake, etc.
                            (defaults to kafka)
    --slack_id <arg>        [Optional] The Slack team id if your protobuf
                            files contain comments with references to
                            channel names. We will translate comments like
                            #data-eng in your protobuf file to slack urls
                            like:
                            https://slack.com/app_redirect?channel=data-en
                            g&team=T1234 following the documentation at
                            (https://api.slack.com/reference/deep-linking#
                            deep-linking-into-your-slack-app__opening-a-ch
                            annel-by-name-or-id) The easiest way to find
                            your Slack team id is to open your workspace
                            in your browser. It should look something
                            like:
                            https://app.slack.com/client/TUMKD5EGJ/...  In
                            this case, the team-id is TUMKD5EGJ.
    --subtype               [Optional] A custom subtype to attach to all
                            entities produced. e.g. event, schema, topic
                            etc.(Default is schema)
    --transport <arg>       [Optional] What transport to use to
                            communicate with DataHub. Options are: rest
                            (default), kafka and file.
```

You can run it like a standard java jar application:
```shell

java -jar build/libs/datahub-protobuf-0.8.45-SNAPSHOT.jar --descriptor ../datahub-protobuf-example/build/descriptors/main.dsc --directory ../datahub-protobuf-example/schema/protobuf/v1/clickstream/ --transport rest
```

or using gradle
```shell
../../../gradlew run --args="--descriptor ../datahub-protobuf-example/build/descriptors/main.dsc --directory ../datahub-protobuf-example/schema/protobuf/v1/clickstream/ --transport rest"
```

Result:
```
java -jar build/libs/datahub-protobuf-0.8.45-SNAPSHOT.jar --descriptor ../datahub-protobuf-example/build/descriptors/main.dsc --directory ../datahub-protobuf-example/schema/protobuf/v1/clickstream/ --transport rest
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
✅ Successfully emitted 90 events for 5 files to DataHub REST
```

You can also route results to a file by using the `--transport file --filename events.json` options.

##### Important Flags
Here are a few important flags to use with this command
- --env : Defaults to DEV, you should use PROD once you have ironed out all the issues with running this command.
- --platform: Defaults to Kafka (as most people use protobuf schema repos with Kafka), but you can provide a custom platform name for this e.g. (`schema_repo` or `<company_name>_schemas`). If you use a custom platform, make sure to provision the custom platform on your DataHub instance with a logo etc, to get a native experience. See how to use the [put platform command](../../../docs/cli.md#put-platform) to accomplish this.
- --subtype : This gives your entities a more descriptive category than Dataset in the UI. Defaults to schema, but you might find topic, event or message more descriptive.



## Example Application (separate project)

The standalone [example project](../datahub-protobuf-example) shows you how you can create an independent project that uses this as part of a build task.

### Sample Usage:

```shell
export DATAHUB_API=...
export DATAHUB_TOKEN=...

# Optional parameters
# export DATAHUB_ENV=PROD
# export DATAHUB_GITHUBORG=datahub-project
# export DATAHUB_SLACKID=

# publishSchema task will publish all the protobuf files into DataHub
./gradlew publishSchema
```