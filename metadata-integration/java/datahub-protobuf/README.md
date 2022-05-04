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

One or more owners can be specified and can be any combination of `corpUser` and `corpGroup` entities. The default entity type is `corpGroup`. By default, the ownership type is set to `producer`, see the second example for setting the ownership type.

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
