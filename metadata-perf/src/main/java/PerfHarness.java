package com.linkedin.metadata.performance;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.aspect.DatasetAspectArray;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestLiResponseException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class PerfHarness {
  private final Logger _logger = LoggerFactory.getLogger("EntityClient");
  private static final String GMS_HOST_ENV_VAR = "GMS_HOST";
  private static final String GMS_PORT_ENV_VAR = "GMS_PORT";
  private static final String GMS_USE_SSL_ENV_VAR = "GMS_SSL";
  private static final String GMS_SSL_PROTOCOL_VAR = "GMS_SSL_PROTOCOL";

  private <T> Response<T> sendClientRequest(Request<T> request) throws RemoteInvocationException {
    try {
      return _client.sendRequest(request).getResponse();
    } catch (RemoteInvocationException e) {
      if (((RestLiResponseException) e).getStatus() == 404) {
        _logger.error("ERROR: Your datahub-frontend instance version is ahead of your gms instance. "
            + "Please update your gms to the latest Datahub release");
        System.exit(1);
      } else {
        throw e;
      }
    }
    return null;
  }

  /**
   @Getter private final Client _client = DefaultRestliClientFactory.getRestLiClient(
   Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR),
   Integer.valueOf(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR)),
   Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, "False")),
   Configuration.getEnvironmentVariable(GMS_SSL_PROTOCOL_VAR));
   **/

  @Getter
  private final Client _client = DefaultRestliClientFactory.getRestLiClient("localhost", 8080, false, null);
  private static final JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec();
  private static PrimitiveIterator.OfInt random = new Random().ints().iterator();

  public static Urn generateUrn(int index) throws URISyntaxException {
    String urnString = "urn:li:dataset:(urn:li:dataPlatform:foo,bar_" + index + ",PROD)";
    return Urn.createFromString(urnString);
  }

  public static DatasetProperties getProperties(int index) {
    DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setDescription("This is a great dataset " + index);
    return datasetProperties;
  }

  @SneakyThrows
  public static Ownership getOwnership(int index) {
    Ownership ownership = new Ownership();
    ownership.setOwners(new OwnerArray(ImmutableList.of(
        new Owner().setOwner(Urn.createFromString("urn:li:corpuser:test" + index)).setType(OwnershipType.DATAOWNER),
        new Owner().setOwner(Urn.createFromString("urn:li:corpuser:haha" + index)).setType(OwnershipType.DATAOWNER))));
    return ownership;
  }

  @SneakyThrows
  public static Snapshot getSnapshot(int index) {
    DatasetSnapshot datasetSnapshot = new DatasetSnapshot();
    datasetSnapshot.setUrn(DatasetUrn.createFromUrn(generateUrn(index)));
    DatasetAspectArray datasetAspects = new DatasetAspectArray();
    datasetAspects.add(DatasetAspect.create(getProperties(index)));
    datasetAspects.add(DatasetAspect.create(getOwnership(index)));
    datasetSnapshot.setAspects(datasetAspects);
    return Snapshot.create(datasetSnapshot);
  }

  @SneakyThrows
  public static MetadataChangeProposal getPropertiesProposal(int index) {
    MetadataChangeProposal gmce = new MetadataChangeProposal();
    gmce.setEntityType("dataset");
    gmce.setAspectName("datasetProperties");
    MetadataChangeProposal.EntityKey key = MetadataChangeProposal.EntityKey.create(generateUrn(index));
    gmce.setEntityKey(key);
    gmce.setChangeType(ChangeType.UPDATE);
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setContentType("application/json");
    DatasetProperties datasetProperties = getProperties(index);
    byte[] datasetProfileSerialized = dataTemplateCodec.dataTemplateToBytes(datasetProperties);
    genericAspect.setValue(ByteString.unsafeWrap(datasetProfileSerialized));
    gmce.setAspect(genericAspect);
    return gmce;
  }

  @SneakyThrows
  public static MetadataChangeProposal getOwnershipProposal(int index) {
    MetadataChangeProposal gmce = new MetadataChangeProposal();
    gmce.setEntityType("dataset");
    gmce.setAspectName("ownership");
    MetadataChangeProposal.EntityKey key = MetadataChangeProposal.EntityKey.create(generateUrn(index));
    gmce.setEntityKey(key);
    gmce.setChangeType(ChangeType.UPDATE);
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setContentType("application/json");
    byte[] datasetProfileSerialized = dataTemplateCodec.dataTemplateToBytes(getOwnership(index));
    genericAspect.setValue(ByteString.unsafeWrap(datasetProfileSerialized));
    gmce.setAspect(genericAspect);
    return gmce;
  }
  enum AspectType {
    REGULAR,
    TIMESERIES
  }

  enum EndPointType {
    SNAPSHOT_INGEST,
    GENERIC_INGEST
  }


  @SneakyThrows
  public static MetadataChangeProposal getTimeseriesProposal(int index) {
    MetadataChangeProposal gmce = new MetadataChangeProposal();
    gmce.setEntityType("dataset");
    gmce.setAspectName("datasetProfile");
    MetadataChangeProposal.EntityKey key = MetadataChangeProposal.EntityKey.create(generateUrn(index));
    gmce.setEntityKey(key);
    gmce.setChangeType(ChangeType.UPDATE);
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setContentType("application/json");
    DatasetProfile datasetProfile = new DatasetProfile();
    datasetProfile.setRowCount(10000);
    datasetProfile.setColumnCount(100);
    byte[] datasetProfileSerialized = dataTemplateCodec.dataTemplateToBytes(datasetProfile);
    genericAspect.setValue(ByteString.unsafeWrap(datasetProfileSerialized));
    gmce.setAspect(genericAspect);
    return gmce;
  }

  public static void warmup(EntityClient entityClient, AspectType aspectType, EndPointType endPointType, int numIterations) throws RemoteInvocationException {
      int index = Math.abs(random.nextInt());
      long startTime = System.nanoTime();
      for (int i = 0; i < numIterations; ++i) {
          executeOneRequest(entityClient, aspectType, endPointType, index);
      }
      reportStats("WarmUp Stats", startTime, numIterations, 0);
  }


  public static void executeOneRequest(EntityClient entityClient, AspectType aspectType, EndPointType endPointType, int index) throws RemoteInvocationException {
    switch (aspectType) {
      case REGULAR: {
        switch (endPointType) {
          case GENERIC_INGEST: {
            Response<Void> response = entityClient.ingestProposal(getPropertiesProposal(index));
            Preconditions.checkState(response.getStatus() == 200);
            response = entityClient.ingestProposal(getOwnershipProposal(index));
            Preconditions.checkState(response.getStatus() == 200);
          } break;
          case SNAPSHOT_INGEST: {
            Response<Void> response = entityClient.update(new com.linkedin.entity.Entity().setValue(getSnapshot(index)));
            Preconditions.checkState(response.getStatus() == 200);
          } break;
        }
      } break;
      case TIMESERIES: {
        switch (endPointType) {
          case GENERIC_INGEST: {
            Response<Void> response = entityClient.ingestProposal(getTimeseriesProposal(index));
            Preconditions.checkState(response.getStatus() == 200);
          } break;
          case SNAPSHOT_INGEST: {
            throw new RuntimeException("Timeseries aspects cannot be sent to Snapshot endpoints");
          }
        }
      }
    }
  }

  public static void main(String[] args)
      throws URISyntaxException, RemoteInvocationException, InterruptedException, IOException {

    PerfHarness harness = new PerfHarness();
    // Perf Scenario Definition
    AspectType aspectType = AspectType.REGULAR;
    EndPointType endPointType = EndPointType.GENERIC_INGEST;
    int numRequests = 100000;



    EntityClient entityClient = new EntityClient(harness._client);
    warmup(entityClient, aspectType, endPointType, 100);
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
    if (numRequests > 0) {
      CountDownLatch latch = new CountDownLatch(numRequests);
      AtomicInteger failedRequests = new AtomicInteger(0);
      AtomicInteger successfulRequests = new AtomicInteger(0);

      long startTime = System.nanoTime();
      for (int i = 0; i < numRequests; ++i) {
        int index = Math.abs(random.nextInt());
        executor.execute(() -> {
          try {
            executeOneRequest(entityClient, aspectType, endPointType, index);
            successfulRequests.incrementAndGet();
          } catch (RemoteInvocationException e) {
            e.printStackTrace();
            failedRequests.incrementAndGet();
          }
          latch.countDown();
        });
      }
      while (!latch.await(30, TimeUnit.SECONDS)) {
        reportStats("Perf", startTime, successfulRequests.get(), failedRequests.get());
      }
      reportStats("Perf", startTime, successfulRequests.get(), failedRequests.get());
    }
    System.exit(0);
  }

  private static void reportStats(String context, long startTime, int successfulRequests, int failedRequests) {
    long endTime = System.nanoTime();
    long durationInMillis = (long) ((endTime - startTime) / 1000000.0);
    long throughput = (successfulRequests + failedRequests) * 1000 / durationInMillis;
    System.out.println(context + ":Processed " + (successfulRequests+failedRequests) + " requests in " + durationInMillis / 1000.0 + " seconds");
    System.out.println(context + ":Successful = " + successfulRequests + "; Failed = " + failedRequests);
    System.out.println(context + ":Aggregate Throughput = " + throughput + " requests/sec");
    System.out.println("------------------------------------------------------------------");
  }
}
