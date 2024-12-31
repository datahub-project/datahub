package datahub.spark.model;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class AppEndEvent extends LineageEvent {

  private final AppStartEvent start;

  public AppEndEvent(String master, String appName, String appId, long time, AppStartEvent start) {
    super(master, appName, appId, time);
    this.start = start;
  }

  @Override
  public List<MetadataChangeProposalWrapper> asMetadataEvents() {
    DataFlowUrn flowUrn = LineageUtils.flowUrn(getMaster(), getAppName());

    StringMap customProps = start.customProps();
    customProps.put("completedAt", timeStr());

    DataFlowInfo flowInfo =
        new DataFlowInfo().setName(getAppName()).setCustomProperties(customProps);

    return Collections.singletonList(
        MetadataChangeProposalWrapper.create(
            b -> b.entityType("dataFlow").entityUrn(flowUrn).upsert().aspect(flowInfo)));
  }
}
