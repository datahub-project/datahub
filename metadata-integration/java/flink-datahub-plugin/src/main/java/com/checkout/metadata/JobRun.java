package com.checkout.metadata;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataprocess.*;
import com.linkedin.metadata.key.DataProcessInstanceKey;
import datahub.event.MetadataChangeProposalWrapper;

import java.net.URISyntaxException;
import java.util.ArrayList;

public class JobRun {
    private final long startTimeMilliseconds;
    private final String jobId;
    private final DataJobUrn dataJobUrn;
    private final Urn dataProcessInstanceUrn;

    /*
        Flink JobRun maps to a DataProcessInstance in DataHub. Here we extract all the information we want to push to DataHub.
     */
    public JobRun(long startTimeMilliseconds, String jobId, DataJobUrn dataJobUrn) {
        this.startTimeMilliseconds = startTimeMilliseconds;
        this.jobId = jobId;
        this.dataJobUrn = dataJobUrn;
        this.dataProcessInstanceUrn = new Urn("dataProcessInstance", new TupleKey(jobId));
    }

    public Urn getUrn() {
        return dataProcessInstanceUrn;
    }

    public ArrayList<MetadataChangeProposalWrapper<?>> getProposals() {
        ArrayList<MetadataChangeProposalWrapper<?>> proposals = new ArrayList<>();
        try {
            proposals.add(constructKey());
            proposals.add(constructStart());
            proposals.add(constructInfo());
            proposals.add(constructParent());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return proposals;
    }

    public ArrayList<MetadataChangeProposalWrapper<?>> getProposalsForExecuted(Throwable throwable) {
        ArrayList<MetadataChangeProposalWrapper<?>> proposals = new ArrayList<>();
        proposals.add(constructEnd(throwable));
        return proposals;
    }

    private MetadataChangeProposalWrapper<?> constructKey() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("dataProcessInstance")
                .entityUrn(dataProcessInstanceUrn)
                .upsert()
                .aspect(new DataProcessInstanceKey().setId(jobId)).build();
    }

    private MetadataChangeProposalWrapper<?> constructStart() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("dataProcessInstance")
                .entityUrn(dataProcessInstanceUrn)
                .upsert()
                .aspect(new DataProcessInstanceRunEvent()
                        .setStatus(DataProcessRunStatus.STARTED)
                        .setDurationMillis(0)
                        .setTimestampMillis(startTimeMilliseconds)
                ).build();
    }

    private MetadataChangeProposalWrapper<?> constructInfo() throws URISyntaxException {
        return MetadataChangeProposalWrapper.builder()
                .entityType("dataProcessInstance")
                .entityUrn(dataProcessInstanceUrn)
                .upsert()
                .aspect(new DataProcessInstanceProperties()
                        .setName(jobId)
                        .setCreated(
                                new AuditStamp().setTime(startTimeMilliseconds)
                                        .setActor(Urn.createFromString("urn:li:corpuser:datahub")))
                        .setType(DataProcessType.STREAMING))
                .build();
    }

    private MetadataChangeProposalWrapper<?> constructParent() {
        return MetadataChangeProposalWrapper.builder()
                .entityType("dataProcessInstance")
                .entityUrn(dataProcessInstanceUrn)
                .upsert()
                .aspect(new DataProcessInstanceRelationships()
                        .setParentTemplate(dataJobUrn)
                        .setUpstreamInstances(new UrnArray()))
                .build();
    }

    private MetadataChangeProposalWrapper<?> constructEnd(Throwable throwable) {
        DataProcessInstanceRunResult dataProcessInstanceRunResult;

        if (throwable != null) {
            dataProcessInstanceRunResult = new DataProcessInstanceRunResult()
                    .setType(RunResultType.FAILURE)
                    .setNativeResultType(throwable.toString());
        } else {
            dataProcessInstanceRunResult = new DataProcessInstanceRunResult()
                    .setType(RunResultType.SUCCESS)
                    .setNativeResultType("FINISHED");
        }

        return MetadataChangeProposalWrapper.builder()
                .entityType("dataProcessInstance")
                .entityUrn(dataProcessInstanceUrn)
                .upsert()
                .aspect(new DataProcessInstanceRunEvent()
                        .setStatus(DataProcessRunStatus.COMPLETE)
                        .setResult(dataProcessInstanceRunResult)
                        .setDurationMillis(System.currentTimeMillis() - startTimeMilliseconds)
                        .setTimestampMillis(System.currentTimeMillis())
                ).build();
    }
}
