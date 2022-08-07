# Java Emitter

In some cases, you might want to construct Metadata events directly and use programmatic ways to emit that metadata to DataHub. Use-cases are typically push-based and include emitting metadata events from CI/CD pipelines, custom orchestrators etc.

The [`io.acryl:datahub-client`](https://mvnrepository.com/artifact/io.acryl/datahub-client) Java package offers REST emitter API-s, which can be easily used to emit metadata from your JVM-based systems. For example, the Spark lineage integration uses the Java emitter to emit metadata events from Spark jobs.


## Installation

Follow the specific instructions for your build system to declare a dependency on the appropriate version of the package. 

**_Note_**: Check the [Maven repository](https://mvnrepository.com/artifact/io.acryl/datahub-client) for the latest version of the package before following the instructions below.

### Gradle
Add the following to your build.gradle.
```gradle
implementation 'io.acryl:datahub-client:__version__'
```
### Maven
Add the following to your `pom.xml`.
```xml
<!-- https://mvnrepository.com/artifact/io.acryl/datahub-client -->
<dependency>
    <groupId>io.acryl</groupId>
    <artifactId>datahub-client</artifactId>
    <!-- replace __version__ with the latest version number -->
    <version>__version__</version>
</dependency>
```

## REST Emitter

The REST emitter is a thin wrapper on top of the [`Apache HttpClient`](https://hc.apache.org/httpcomponents-client-4.5.x/index.html) library. It supports non-blocking emission of metadata and handles the details of JSON serialization of metadata aspects over the wire.

Constructing a REST Emitter follows a lambda-based fluent builder pattern. The config parameters mirror the Python emitter [configuration](../../metadata-ingestion/sink_docs/datahub.md#config-details) for the most part. In addition, you can also customize the HttpClient that is constructed under the hood by passing in customizations to the HttpClient builder.
```java
import datahub.client.rest.RestEmitter;
//...
RestEmitter emitter = RestEmitter.create(b -> b
                                              .server("http://localhost:8080")
//Auth token for Managed DataHub              .token(AUTH_TOKEN_IF_NEEDED)
//Override default timeout of 10 seconds      .timeoutSec(OVERRIDE_DEFAULT_TIMEOUT_IN_SECONDS)
//Add additional headers                      .extraHeaders(Collections.singletonMap("Session-token", "MY_SESSION"))
// Customize HttpClient's connection ttl      .customizeHttpAsyncClient(c -> c.setConnectionTimeToLive(30, TimeUnit.SECONDS))
                                    );
```

### Usage

```java
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.client.rest.RestEmitter;
import datahub.client.Callback;
// ... followed by

// Creates the emitter with the default coordinates and settings
RestEmitter emitter = RestEmitter.createWithDefaults(); 

MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table,PROD)")
        .upsert()
        .aspect(new DatasetProperties().setDescription("This is the canonical User profile dataset"))
        .build();

// Blocking call using future
Future<MetadataWriteResponse> requestFuture = emitter.emit(mcpw, null).get();

// Non-blocking using callback
emitter.emit(mcpw, new Callback() {
      @Override
      public void onCompletion(MetadataWriteResponse response) {
        if (response.isSuccess()) {
          System.out.println(String.format("Successfully emitted metadata event for %s", mcpw.getEntityUrn()));
        } else {
          // Get the underlying http response
          HttpResponse httpResponse = (HttpResponse) response.getUnderlyingResponse();
          System.out.println(String.format("Failed to emit metadata event for %s, aspect: %s with status code: %d",
              mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));
          // Print the server side exception if it was captured
          if (response.getServerException() != null) {
            System.out.println(String.format("Server side exception was %s", response.getServerException()));
          }
        }
      }

      @Override
      public void onFailure(Throwable exception) {
        System.out.println(
            String.format("Failed to emit metadata event for %s, aspect: %s due to %s", mcpw.getEntityUrn(),
                mcpw.getAspectName(), exception.getMessage()));
      }
    });
```

### REST Emitter Code

If you're interested in looking at the REST emitter code, it is available [here](./datahub-client/src/main/java/datahub/client/rest/RestEmitter.java).

## Kafka Emitter

The Kafka emitter is a thin wrapper on top of the SerializingProducer class from `confluent-kafka` and offers a non-blocking interface for sending metadata events to DataHub. Use this when you want to decouple your metadata producer from the uptime of your datahub metadata server by utilizing Kafka as a highly available message bus. For example, if your DataHub metadata service is down due to planned or unplanned outages, you can still continue to collect metadata from your mission critical systems by sending it to Kafka. Also use this emitter when throughput of metadata emission is more important than acknowledgement of metadata being persisted to DataHub's backend store.

**_Note_**: The Kafka emitter uses Avro to serialize the Metadata events to Kafka. Changing the serializer will result in unprocessable events as DataHub currently expects the metadata events over Kafka to be serialized in Avro.


### Usage

```java


import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.linkedin.dataset.DatasetProperties;
import datahub.client.kafka.KafkaEmitter;
import datahub.client.kafka.KafkaEmitterConfig;
import datahub.event.MetadataChangeProposalWrapper;

// ... followed by

// Creates the emitter with the default coordinates and settings
KafkaEmitterConfig.KafkaEmitterConfigBuilder builder = KafkaEmitterConfig.builder(); KafkaEmitterConfig config = builder.build();
KafkaEmitter emitter = new KafkaEmitter(config);
 
//Test if topic is available

if(emitter.testConnection()){
 
	MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
	        .entityType("dataset")
	        .entityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table,PROD)")
	        .upsert()
	        .aspect(new DatasetProperties().setDescription("This is the canonical User profile dataset"))
	        .build();
	
	// Blocking call using future
	Future<MetadataWriteResponse> requestFuture = emitter.emit(mcpw, null).get();
	
	// Non-blocking using callback
	emitter.emit(mcpw, new Callback() {
	
	      @Override
	      public void onFailure(Throwable exception) {
	        System.out.println("Failed to send with: " + exception);
	      }
	      @Override
	      public void onCompletion(MetadataWriteResponse metadataWriteResponse) {
	        if (metadataWriteResponse.isSuccess()) {
	          RecordMetadata metadata = (RecordMetadata) metadataWriteResponse.getUnderlyingResponse();
	          System.out.println("Sent successfully over topic: " + metadata.topic());
	        } else {
	          System.out.println("Failed to send with: " + metadataWriteResponse.getUnderlyingResponse());
	        }
	      }
	    });

}
else {
	System.out.println("Kafka service is down.");
}
```
### Kafka Emitter Code

If you're interested in looking at the Kafka emitter code, it is available [here](./datahub-client/src/main/java/datahub/client/kafka/KafkaEmitter.java).

## File Emitter

The File emitter writes metadata change proposal events (MCPs) into a JSON file that can be later handed off to the Python [File source](docs/generated/ingestion/sources/file.md) for ingestion. This works analogous to the [File sink](../../metadata-ingestion/sink_docs/file.md) in Python. This mechanism can be used when the system producing metadata events doesn't have direct connection to DataHub's REST server or Kafka brokers. The generated JSON file can be transferred later and then ingested into DataHub using the [File source](docs/generated/ingestion/sources/file.md).

### Usage

```java


import datahub.client.file.FileEmitter;
import datahub.client.file.FileEmitterConfig;
import datahub.event.MetadataChangeProposalWrapper;

// ... followed by


// Define output file co-ordinates
String outputFile = "/my/path/output.json";

//Create File Emitter
FileEmitter emitter = new FileEmitter(FileEmitterConfig.builder().fileName(outputFile).build());

// A couple of sample metadata events
MetadataChangeProposalWrapper mcpwOne = MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table,PROD)")
        .upsert()
        .aspect(new DatasetProperties().setDescription("This is the canonical User profile dataset"))
        .build();

MetadataChangeProposalWrapper mcpwTwo = MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.fact-orders-table,PROD)")
        .upsert()
        .aspect(new DatasetProperties().setDescription("This is the canonical Fact table for orders"))
        .build();

MetadataChangeProposalWrapper[] mcpws = { mcpwOne, mcpwTwo };
for (MetadataChangeProposalWrapper mcpw : mcpws) {
   emitter.emit(mcpw);
}
emitter.close(); // calling close() is important to ensure file gets closed cleanly
    
```
### File Emitter Code

If you're interested in looking at the File emitter code, it is available [here](./datahub-client/src/main/java/datahub/client/file/FileEmitter.java).

### Support for S3, GCS etc.

The File emitter only supports writing to the local filesystem currently. If you're interested in adding support for S3, GCS etc., contributions are welcome! 

## Other Languages

Emitter API-s are also supported for:
- [Python](../../metadata-ingestion/as-a-library.md)


