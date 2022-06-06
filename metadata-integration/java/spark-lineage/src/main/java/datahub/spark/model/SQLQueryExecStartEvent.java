package datahub.spark.model;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.StringMap;
import datahub.spark.model.dataset.SparkDataset;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.datajob.JobStatus;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
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
  public List<MetadataChangeProposalWrapper> asMetadataEvents() {
    DataJobUrn jobUrn = jobUrn();
    MetadataChangeProposalWrapper mcpJobIO =
        MetadataChangeProposalWrapper.create(b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(jobIO()));

    DataJobInfo jobInfo = jobInfo();
    jobInfo.setCustomProperties(customProps());
    jobInfo.setStatus(JobStatus.IN_PROGRESS);

    MetadataChangeProposalWrapper mcpJobInfo =
        MetadataChangeProposalWrapper.create(b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(jobInfo));

    return Arrays.asList(mcpJobIO, mcpJobInfo);
  }

  DataJobInfo jobInfo() {
    return new DataJobInfo().setName(datasetLineage.getCallSiteShort()).setType(DataJobInfo.Type.create("sparkJob"));
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

  public DatasetUrnArray getOuputDatasets() {
    DatasetUrnArray out = new DatasetUrnArray();
    out.add(datasetLineage.getSink().urn());
    return out;
  }

  public DatasetUrnArray getInputDatasets() {
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

    return in;
  }

  private DataJobInputOutput jobIO() {
    DataJobInputOutput io = new DataJobInputOutput().setInputDatasets(getInputDatasets())
        .setOutputDatasets(getOuputDatasets());
    return io;
  }
}