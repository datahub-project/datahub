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
  public static Snapshot getRandomSnapshot() {
    int index = Math.abs(random.nextInt());
    DatasetSnapshot datasetSnapshot = new DatasetSnapshot();
    datasetSnapshot.setUrn(DatasetUrn.createFromUrn(generateUrn(index)));
    DatasetAspectArray datasetAspects = new DatasetAspectArray();
    datasetAspects.add(DatasetAspect.create(getProperties(index)));
    datasetAspects.add(DatasetAspect.create(getOwnership(index)));
    datasetSnapshot.setAspects(datasetAspects);
    return Snapshot.create(datasetSnapshot);
  }

  @SneakyThrows
  public static MetadataChangeProposal getPropertiesProposal() {
    int index = Math.abs(random.nextInt());
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
  public static MetadataChangeProposal getOwnershipProposal() {
    int index = Math.abs(random.nextInt());
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

  @SneakyThrows
  public static MetadataChangeProposal getTimeseriesProposal() {
    int index = Math.abs(random.nextInt());
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

  public static void main(String[] args)
      throws URISyntaxException, RemoteInvocationException, InterruptedException, IOException {

    PerfHarness harness = new PerfHarness();
    EntityClient entityClient = new EntityClient(harness._client);
    {
      long startTime = System.nanoTime();
      entityClient.ingestProposal(getPropertiesProposal());
      entityClient.ingestProposal(getOwnershipProposal());
//      entityClient.update(new com.linkedin.entity.Entity().setValue(getRandomSnapshot()));
      long endTime = System.nanoTime();
      long durationInMillis = (long) ((endTime - startTime) / 1000000.0);
      System.out.println("Duration of one call was: " + durationInMillis);
    }

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
    int numRequests = 1000;
    if (numRequests > 0) {
      CountDownLatch latch = new CountDownLatch(numRequests);
      AtomicInteger failedRequests = new AtomicInteger(0);
      AtomicInteger successfulRequests = new AtomicInteger(0);

      long startTime = System.nanoTime();
      for (int i = 0; i < numRequests; ++i) {
        executor.execute(() -> {
          try {

//            Response<Void> response = entityClient.update(new com.linkedin.entity.Entity().setValue(getRandomSnapshot()));
              Response<Void> response = entityClient.ingestProposal(getPropertiesProposal());
            Preconditions.checkState(response.getStatus() == 200);
              response = entityClient.ingestProposal(getOwnershipProposal());
            Preconditions.checkState(response.getStatus() == 200);
            successfulRequests.incrementAndGet();
          } catch (RemoteInvocationException e) {
            e.printStackTrace();
            failedRequests.incrementAndGet();
          }
          latch.countDown();
        });
      }
      latch.await();
      long endTime = System.nanoTime();
      long durationInMillis = (long) ((endTime - startTime) / 1000000.0);
      long throughput = numRequests * 1000 / durationInMillis;
      System.out.println("Processed " + numRequests + " requests in " + durationInMillis / 1000.0 + " seconds");
      System.out.println("Successful = " + successfulRequests.get() + "; Failed = " + failedRequests.get());
      System.out.println("Throughput = " + throughput + " requests/sec");
    }
    System.exit(0);
  }
}
