package com.linkedin.metadata.performance;

import com.google.common.base.Preconditions;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.aspect.DatasetAspectArray;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.metadata.snapshot.ChartSnapshot;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.util.AspectDeserializationUtil;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.*;
import lombok.Getter;
import lombok.SneakyThrows;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;


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
    @Getter
    private final Client _client = DefaultRestliClientFactory.getRestLiClient(
            Configuration.getEnvironmentVariable(GMS_HOST_ENV_VAR),
            Integer.valueOf(Configuration.getEnvironmentVariable(GMS_PORT_ENV_VAR)),
            Boolean.parseBoolean(Configuration.getEnvironmentVariable(GMS_USE_SSL_ENV_VAR, "False")),
            Configuration.getEnvironmentVariable(GMS_SSL_PROTOCOL_VAR));
    **/

    @Getter
    private final Client _client = DefaultRestliClientFactory.getRestLiClient(
            "localhost",
            8080,
            false,
            null);
    private static final JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec();
    private static PrimitiveIterator.OfInt random = new Random().ints().iterator();
    public static Urn generateRandomUrn() throws URISyntaxException {
        String urnString = "urn:li:dataset:(urn:li:dataPlatform:foo,bar_"+Math.abs(random.next())+",PROD)";
        return Urn.createFromString(urnString);
    }

    @SneakyThrows
    public static Snapshot getRandomSnapshot()  {
        DatasetSnapshot datasetSnapshot = new DatasetSnapshot();
        datasetSnapshot.setUrn(DatasetUrn.createFromUrn(generateRandomUrn()));
        DatasetAspectArray datasetAspects = new DatasetAspectArray();
        DatasetProperties datasetProperties = new DatasetProperties();
        datasetProperties.setDescription("This is a great dataset");
        datasetAspects.add(DatasetAspect.create(datasetProperties));
        datasetSnapshot.setAspects(datasetAspects);
        final Snapshot snapshot = Snapshot.create(datasetSnapshot);
        return snapshot;
    }

    @SneakyThrows
    public static MetadataChangeProposal getRandomMCP() {
        MetadataChangeProposal gmce = new MetadataChangeProposal();
        gmce.setEntityType("dataset");
        gmce.setAspectName("datasetProfile");
        MetadataChangeProposal.EntityKey key = MetadataChangeProposal.EntityKey.create(generateRandomUrn());
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

    public static void main(String[] args) throws URISyntaxException, RemoteInvocationException, InterruptedException, IOException {

        PerfHarness harness = new PerfHarness();
        EntityClient entityClient = new EntityClient(harness._client);
        {
            long startTime = System.nanoTime();
            entityClient.ingestProposal(getRandomMCP());
            //entityClient.update(new com.linkedin.entity.Entity().setValue(getRandomSnapshot()));
            long endTime = System.nanoTime();
            long durationInMillis = (long) ((endTime - startTime)/1000000.0);
            System.out.println("Duration of one call was: " + durationInMillis);
        }


        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(100);
        int numRequests =  100000;
        if (numRequests > 0) {
            CountDownLatch latch = new CountDownLatch(numRequests);
            AtomicInteger failedRequests = new AtomicInteger(0);
            AtomicInteger successfulRequests = new AtomicInteger(0);

            long startTime = System.nanoTime();
            for (int i = 0; i < numRequests; ++i) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {

                            //Response<Void> response = entityClient.update(new com.linkedin.entity.Entity().setValue(getRandomSnapshot()));
                            Response<Void> response = entityClient.ingestProposal(getRandomMCP());
                            Preconditions.checkState(response.getStatus() == 200);
                            successfulRequests.incrementAndGet();
                        } catch (RemoteInvocationException e) {
                            e.printStackTrace();
                            failedRequests.incrementAndGet();
                        }
                        latch.countDown();
                    }
                });
            }
            latch.await();
            long endTime = System.nanoTime();
            long durationInMillis = (long) ((endTime - startTime) / 1000000.0);
            long throughput = numRequests * 1000 / durationInMillis;
            System.out.println("Processed " + numRequests + " requests in " + durationInMillis/1000.0 + " seconds");
            System.out.println("Successful = " + successfulRequests.get() + "; Failed = " + failedRequests.get());
            System.out.println("Throughput = " + throughput + " requests/sec");
        }
        System.exit(0);
    }
}
