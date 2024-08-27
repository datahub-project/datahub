package datahub.spark.model;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.typesafe.config.Config;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@Getter
public class AppStartEvent extends LineageEvent {

  private static final String PLATFORM_INSTANCE_KEY = "platformInstance";
  private static final String PLATFORM_SPARK = "spark";
  private final String sparkUser;
  private Config pipelineConfig;

  public AppStartEvent(
      String master,
      String appName,
      String appId,
      long time,
      String sparkUser,
      Config pipelineConfig) {
    super(master, appName, appId, time);
    this.sparkUser = sparkUser;
    this.pipelineConfig = pipelineConfig;
  }

  public DataFlowUrn getFlowUrn() {
    return LineageUtils.flowUrn(getMaster(), getAppName());
  }

  @Override
  public List<MetadataChangeProposalWrapper> asMetadataEvents() {
    ArrayList<MetadataChangeProposalWrapper> mcps = new ArrayList<MetadataChangeProposalWrapper>();

    if (this.pipelineConfig.hasPath(PLATFORM_INSTANCE_KEY)) {
      try {
        DataPlatformInstance dpi =
            new DataPlatformInstance()
                .setPlatform(new DataPlatformUrn(PLATFORM_SPARK))
                .setInstance(
                    LineageUtils.dataPlatformInstanceUrn(
                        PLATFORM_SPARK, this.pipelineConfig.getString(PLATFORM_INSTANCE_KEY)));
        mcps.add(
            MetadataChangeProposalWrapper.create(
                b -> b.entityType("dataFlow").entityUrn(getFlowUrn()).upsert().aspect(dpi)));
      } catch (URISyntaxException e) {
        // log error, but don't impact thread
        StringWriter s = new StringWriter();
        PrintWriter p = new PrintWriter(s);
        e.printStackTrace(p);
        log.error(s.toString());
        p.close();
      }
    }
    DataFlowInfo flowInfo =
        new DataFlowInfo().setName(getAppName()).setCustomProperties(customProps());
    mcps.add(
        MetadataChangeProposalWrapper.create(
            b -> b.entityType("dataFlow").entityUrn(getFlowUrn()).upsert().aspect(flowInfo)));
    return mcps;
  }

  StringMap customProps() {
    StringMap customProps = new StringMap();
    customProps.put("startedAt", timeStr());
    customProps.put("appId", getAppId());
    customProps.put("appName", getAppName());
    customProps.put("sparkUser", sparkUser);
    return customProps;
  }
}
