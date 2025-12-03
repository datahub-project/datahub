package com.checkout;

import com.checkout.lineage.LineageParser;
import com.checkout.metadata.Cluster;
import com.checkout.metadata.Job;
import com.checkout.metadata.JobRun;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.Urn;
import datahub.client.Callback;
import datahub.client.Emitter;
import datahub.client.MetadataWriteResponse;
import datahub.event.MetadataChangeProposalWrapper;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DatahubJobListener implements JobListener {
    private static final Logger logger = LoggerFactory.getLogger(DatahubJobListener.class);
    private final Emitter emitter;

    private final Configuration configuration;
    private long startTimeMilliseconds;
    private final LineageParser lineageParser;
    private String jobId;


    private DatahubJobListener(
            Configuration configuration,
            LineageParser lineageParser,
            Emitter emitter) {

        this.configuration = configuration;
        this.lineageParser = lineageParser;

        this.emitter = emitter;
    }

    @Builder(builderMethodName = "listenerBuilder")
    private static DatahubJobListener createDatahubJobListener(
            @NonNull Configuration configuration,
            @Nullable List<Transformation<?>> transformations,
            @NonNull Emitter emitter,
            @Nullable DatasetUrnArray upstreamDatasetsOverride,
            @Nullable DatasetUrnArray upstreamDatasetsAppend,
            @Nullable DatasetUrnArray downstreamDatasetsOverride,
            @Nullable DatasetUrnArray downstreamDatasetsAppend
    ) {

        List<Transformation<?>> safeTransformations = (transformations != null) ? transformations : Collections.emptyList();


        LineageParser parser = new LineageParser(null,
                upstreamDatasetsOverride,
                upstreamDatasetsAppend,
                downstreamDatasetsOverride,
                downstreamDatasetsAppend);
        try {
            ArrayList<Transformation<?>> allTransformations = getAllTransformations(safeTransformations);
            parser = new LineageParser(allTransformations,
                    upstreamDatasetsOverride,
                    upstreamDatasetsAppend,
                    downstreamDatasetsOverride,
                    downstreamDatasetsAppend);
            logger.info("Lineage parsing enabled. Parser created.");
        } catch (Exception e) {
            logger.error("Failed to initialize LineageParser, lineage will be disabled.", e);
        }

        return new DatahubJobListener(
                configuration,
                parser,
                emitter);
    }

    public static DatahubJobListenerBuilder builder() {
        return listenerBuilder();
    }


    public static ArrayList<Transformation<?>> getAllTransformations(List<Transformation<?>> inputTransformations) {
        logger.info("Getting all transformations from input: {}", ArrayUtils.toString(inputTransformations));
        ArrayList<Transformation<?>> outputTransformations = new ArrayList<>();
        for (Transformation<?> transformation : inputTransformations) {
            outputTransformations.add(transformation);
            outputTransformations.addAll(getAllTransformations(transformation.getInputs()));
        }
        logger.info("All Transformations: {}", ArrayUtils.toString(outputTransformations));
        return outputTransformations;
    }

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        logger.info("Job submitted");
        try {
            this.startTimeMilliseconds = System.currentTimeMillis();
            sendJobMetadataToDatahub(jobClient);
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Failed to send job metadata to Datahub", e);
            throw new RuntimeException(e);
        } catch (URISyntaxException | NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public void sendLineage(Urn dataJobUrn) {
        for (MetadataChangeProposalWrapper<?> proposal : lineageParser.getProposals(dataJobUrn)) {
            logger.info("Sending lineage proposal: {}", proposal);
            emit(proposal);
        }
        logger.info("Sent lineage to Datahub");
    }

    @SneakyThrows
    private void emit(MetadataChangeProposalWrapper<?> proposal) {
        logger.info("Emitting lineage proposal: {}", proposal);

        Callback callback = new Callback() {
            @Override
            public void onCompletion(MetadataWriteResponse response) {
                logger.info("Emitted lineage proposal: {}", proposal);
            }

            @Override
            public void onFailure(Throwable exception) {
                logger.error("Failed to emit lineage proposal: {}", proposal);
            }
        };
        try {
            emitter.emit(proposal, callback);
        } catch (Exception e) {
            logger.error("");
        }
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        logger.info("Job finished! {}", jobExecutionResult);
        JobRun jobRun = new JobRun(startTimeMilliseconds, jobId, null);
        jobRun.getProposalsForExecuted(throwable).forEach(this::emit);
    }

    private void sendJobMetadataToDatahub(JobClient jobClient) throws IOException, URISyntaxException, NoSuchFieldException, ClassNotFoundException, IllegalAccessException {
        logger.info("Sending job metadata to Datahub");
        jobId = jobClient.getJobID().toString();
        Cluster cluster = new Cluster(configuration, startTimeMilliseconds);
        logger.info("Created cluster");
        cluster.getProposals().forEach(this::emit);
        logger.info("Sent job {} metadata to Datahub", cluster.getUrn());

        Job job = new Job(configuration, startTimeMilliseconds, cluster.getUrn());
        job.getProposals().forEach(this::emit);

        logger.info("Sent job {} metadata to Datahub", job.getUrn());

        JobRun jobRun = new JobRun(startTimeMilliseconds, jobId, job.getUrn());
        jobRun.getProposals().forEach(this::emit);
        logger.info("Sent job {} metadata to Datahub", jobRun.getUrn());
        sendLineage(job.getUrn());
    }
}
