# Protobuf Integration

This module is designed to be used with the Java Emitter, the input is a compiled protobuf binary `*.protoc` files and optionally the corresponding `*.proto` source code. In addition, you can supply the root message in cases where a single protobuf source file includes multiple non-nested messages.

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
  PROPERTY = 0; // Datahub Custom Property
  TAG      = 1; // Datahub Tag
  TERM     = 2; // Datahub Term
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
    string team = 5003 [(meta.fld.type) = TAG, (meta.fld.type) = PROPERTY];

    string domain = 60003 [(meta.fld.type) = TAG, (meta.fld.type) = PROPERTY];
    meta.MetaEnumExample type = 60004 [(meta.fld.type) = TAG, (meta.fld.type) = PROPERTY];
    bool bool_feature = 60005 [(meta.fld.type) = TAG];
    string alert_channel = 60007 [(meta.fld.type) = PROPERTY];
  }
}
```

## Gradle Integration

An example application is included which works with the `protobuf-gradle-plugin`, see the standalone [example project](../datahub-protobuf-example).

### Usage

Using the 

```shell
export DATAHUB_API=...
export DATAHUB_TOKEN=...

# Optional parameters
# export DATAHUB_ENV=PROD
# export DATAHUB_GITHUBORG=datahub-project
# export DATAHUB_SLACKID=

./gradlew publishSchema
```
