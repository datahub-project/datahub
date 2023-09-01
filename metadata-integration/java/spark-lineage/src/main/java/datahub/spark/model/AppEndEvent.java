package datahub.spark.model;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.common.Status;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.ArrayList;
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
    ArrayList<MetadataChangeProposalWrapper> mcps = new ArrayList<MetadataChangeProposalWrapper>();

    DataFlowUrn flowUrn = LineageUtils.flowUrn(getMaster(), getAppName());

    StringMap customProps = start.customProps();
    customProps.put("completedAt", timeStr());

    DataFlowInfo flowInfo = new DataFlowInfo().setName(getAppName()).setCustomProperties(customProps);

    mcps.add(MetadataChangeProposalWrapper.create(
        b -> b.entityType("dataFlow").entityUrn(flowUrn).upsert().aspect(flowInfo)));

    // set remove status to false to avoid null status
    Status statusInfo = new Status().setRemoved(false);
    mcps.add(MetadataChangeProposalWrapper.create(
        b -> b.entityType("dataFlow").entityUrn(flowUrn).upsert().aspect(statusInfo).aspectName("status")));

    return mcps;
  }
}