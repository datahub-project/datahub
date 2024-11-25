package datahub.spark.consumer.impl;

import com.linkedin.common.DataJobUrnArray;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.datajob.JobStatus;
import com.typesafe.config.Config;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.model.AppEndEvent;
import datahub.spark.model.AppStartEvent;
import datahub.spark.model.LineageEvent;
import datahub.spark.model.SQLQueryExecStartEvent;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoalesceJobsEmitter extends McpEmitter {

  private static final String PARENT_JOB_KEY = "parent.datajob_urn";
  private final String parentJobUrn;
  private AppStartEvent appStartEvent = null;
  private final ArrayList<SQLQueryExecStartEvent> sqlQueryExecStartEvents = new ArrayList<>();

  public CoalesceJobsEmitter(Config datahubConf) {
    super(datahubConf);
    parentJobUrn =
        datahubConf.hasPath(PARENT_JOB_KEY) ? datahubConf.getString(PARENT_JOB_KEY) : null;
    log.info("CoalesceJobsEmitter initialised with " + PARENT_JOB_KEY + ":" + parentJobUrn);
  }

  @Override
  public void accept(LineageEvent evt) {
    if (evt instanceof AppStartEvent) {
      this.appStartEvent = (AppStartEvent) evt;
      log.debug("AppstartEvent received for processing: " + appStartEvent.getAppId());
      emit(this.appStartEvent.asMetadataEvents());
    } else if (evt instanceof SQLQueryExecStartEvent) {
      SQLQueryExecStartEvent sqlQueryExecStartEvent = (SQLQueryExecStartEvent) evt;
      sqlQueryExecStartEvents.add(sqlQueryExecStartEvent);
      log.debug(
          "SQLQueryExecStartEvent received for processing. for app: "
              + sqlQueryExecStartEvent.getAppId()
              + ":"
              + sqlQueryExecStartEvent.getAppName()
              + "sqlID: "
              + sqlQueryExecStartEvent.getSqlQueryExecId());
    } else if (evt instanceof AppEndEvent) {
      AppEndEvent appEndEvent = (AppEndEvent) evt;
      if (appStartEvent == null) {
        log.error(
            "Application End event received for processing but start event is not received for processing for "
                + appEndEvent.getAppId()
                + "-"
                + appEndEvent.getAppName());
        return;
      }
      log.debug("AppEndEvent received for processing. for app start :" + appEndEvent.getAppId());
      emit(appEndEvent.asMetadataEvents());
      emit(squashSQLQueryExecStartEvents(appEndEvent));
    }
  }

  private List<MetadataChangeProposalWrapper> squashSQLQueryExecStartEvents(
      AppEndEvent appEndEvent) {

    DataJobUrn jobUrn = new DataJobUrn(appStartEvent.getFlowUrn(), appStartEvent.getAppName());

    Set<DatasetUrn> inSet = new TreeSet<DatasetUrn>(new DataSetUrnComparator());
    sqlQueryExecStartEvents.forEach(x -> inSet.addAll(x.getInputDatasets()));
    Set<DatasetUrn> outSet = new TreeSet<DatasetUrn>(new DataSetUrnComparator());
    sqlQueryExecStartEvents.forEach(x -> outSet.addAll(x.getOuputDatasets()));
    DataJobUrnArray upStreamjobs = new DataJobUrnArray();
    try {
      if (parentJobUrn != null) {
        upStreamjobs = new DataJobUrnArray(DataJobUrn.createFromString(parentJobUrn));
      }

    } catch (URISyntaxException e) {
      log.warn(PARENT_JOB_KEY + " is not a valid URN. Skipping setting up upstream job.");
    } catch (ClassCastException e) {
      log.warn(PARENT_JOB_KEY + " is not a valid Datajob URN. Skipping setting up upstream job.");
    }

    DataJobInputOutput jobio =
        new DataJobInputOutput()
            .setInputDatasets(new DatasetUrnArray(inSet))
            .setOutputDatasets(new DatasetUrnArray(outSet))
            .setInputDatajobs(upStreamjobs);

    MetadataChangeProposalWrapper<?> mcpJobIO =
        MetadataChangeProposalWrapper.create(
            b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(jobio));

    StringMap customProps = new StringMap();
    customProps.put("startedAt", appStartEvent.timeStr());
    customProps.put("appId", appStartEvent.getAppId());
    customProps.put("appName", appStartEvent.getAppName());
    customProps.put("completedAt", appEndEvent.timeStr());

    DataJobInfo jobInfo =
        new DataJobInfo()
            .setName(appStartEvent.getAppName())
            .setType(DataJobInfo.Type.create("sparkJob"));
    jobInfo.setCustomProperties(customProps);
    jobInfo.setStatus(JobStatus.COMPLETED);
    MetadataChangeProposalWrapper<?> mcpJobInfo =
        MetadataChangeProposalWrapper.create(
            b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(jobInfo));

    return Arrays.asList(mcpJobIO, mcpJobInfo);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}

class DataSetUrnComparator implements Comparator<DatasetUrn> {

  @Override
  public int compare(DatasetUrn urn1, DatasetUrn urn2) {
    return urn1.toString().compareTo(urn2.toString());
  }
}
