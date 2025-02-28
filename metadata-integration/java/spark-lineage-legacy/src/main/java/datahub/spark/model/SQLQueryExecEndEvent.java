package datahub.spark.model;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataJobInfo;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class SQLQueryExecEndEvent extends LineageEvent {

  private final long sqlQueryExecId;
  private final SQLQueryExecStartEvent start;

  public SQLQueryExecEndEvent(
      String master,
      String appName,
      String appId,
      long time,
      long sqlQueryExecId,
      SQLQueryExecStartEvent start) {
    super(master, appName, appId, time);
    this.sqlQueryExecId = sqlQueryExecId;
    this.start = start;
  }

  @Override
  public List<MetadataChangeProposalWrapper> asMetadataEvents() {
    DataJobUrn jobUrn = start.jobUrn();
    StringMap customProps = start.customProps();
    customProps.put("completedAt", timeStr());

    DataJobInfo jobInfo = start.jobInfo().setCustomProperties(customProps);

    return Collections.singletonList(
        MetadataChangeProposalWrapper.create(
            b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(jobInfo)));
  }
}
