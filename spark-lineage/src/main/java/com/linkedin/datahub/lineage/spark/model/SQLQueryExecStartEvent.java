package com.linkedin.datahub.lineage.spark.model;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.lineage.spark.interceptor.LineageUtils;
import com.linkedin.datahub.lineage.spark.model.dataset.SparkDataset;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.datajob.JobStatus;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.MetadataChangeProposal;

import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class SQLQueryExecStartEvent extends LineageEvent {
  private final long sqlQueryExecId;
  private final DatasetLineage datasetLineage;

  public SQLQueryExecStartEvent(String master, String appName, String appId, long time, long sqlQueryExecId,
      DatasetLineage datasetLineage) {
    super(master, appName, appId, time);
    this.sqlQueryExecId = sqlQueryExecId;
    this.datasetLineage = datasetLineage;
  }

  @Override
  public List<MetadataChangeProposal> toMcps() {
    DataJobUrn jobUrn = jobUrn();
    MetadataChangeProposal mcpJobIO = new MetadataChangeProposal();
    mcpJobIO.setAspectName("dataJobInputOutput");
    mcpJobIO.setAspect(LineageUtils.serializeAspect(jobIO()));
    mcpJobIO.setEntityUrn(jobUrn);
    mcpJobIO.setEntityType("dataJob");
    mcpJobIO.setChangeType(ChangeType.UPSERT);

    DataJobInfo jobInfo = jobInfo();
    jobInfo.setCustomProperties(customProps());
    jobInfo.setStatus(JobStatus.IN_PROGRESS);

    MetadataChangeProposal mcpJobInfo = new MetadataChangeProposal();
    mcpJobInfo.setAspectName("dataJobInfo");
    mcpJobInfo.setAspect(LineageUtils.serializeAspect(jobInfo));
    mcpJobInfo.setEntityUrn(jobUrn);
    mcpJobInfo.setEntityType("dataJob");
    mcpJobInfo.setChangeType(ChangeType.UPSERT);

    return Arrays.asList(mcpJobIO, mcpJobInfo);
  }

  DataJobInfo jobInfo() {
    return new DataJobInfo()
        .setName(datasetLineage.getCallSiteShort())
        .setType(DataJobInfo.Type.create("sparkJob"));
  }

  DataJobUrn jobUrn() {
    /* This is for generating urn from a hash of the plan */
    /*
     * Set<String> sourceUrns = datasetLineage.getSources() .parallelStream() .map(x
     * -> x.urn().toString()) .collect(Collectors.toSet()); sourceUrns = new
     * TreeSet<>(sourceUrns); //sort for consistency
     * 
     * String sinkUrn = datasetLineage.getSink().urn().toString(); String plan =
     * LineageUtils.scrubPlan(datasetLineage.getPlan()); String id =
     * Joiner.on(",").join(sinkUrn, sourceUrns, plan);
     * 
     * return new DataJobUrn(flowUrn(), "planHash_" + LineageUtils.hash(id));
     */
    return new DataJobUrn(flowUrn(), "QueryExecId_" + sqlQueryExecId);
  }

  DataFlowUrn flowUrn() {
    return LineageUtils.flowUrn(getMaster(), getAppName());
  }

  StringMap customProps() {
    StringMap customProps = new StringMap();
    customProps.put("startedAt", timeStr());
    customProps.put("description", datasetLineage.getCallSiteShort());
    customProps.put("SQLQueryId", Long.toString(sqlQueryExecId));
    customProps.put("appId", getAppId());
    customProps.put("appName", getAppName());
    customProps.put("queryPlan", datasetLineage.getPlan());
    return customProps;
  }

  private DataJobInputOutput jobIO() {
    DatasetUrnArray out = new DatasetUrnArray();
    out.add(datasetLineage.getSink().urn());

    DatasetUrnArray in = new DatasetUrnArray();

    Set<SparkDataset> sources = new TreeSet<>(new Comparator<SparkDataset>() {
      @Override
      public int compare(SparkDataset x, SparkDataset y) {
        return x.urn().toString().compareTo(y.urn().toString());
      }

    });
    sources.addAll(datasetLineage.getSources()); // maintain ordering
    for (SparkDataset source : sources) {
      in.add(source.urn());
    }

    DataJobInputOutput io = new DataJobInputOutput().setInputDatasets(in).setOutputDatasets(out);
    return io;
  }
}