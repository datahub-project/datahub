package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public final class AzkabanFlowUrn extends Urn {

  public static final String ENTITY_TYPE = "azkabanFlow";

  private static final String CONTENT_FORMAT = "(%s,%s,%s)";

  private final String clusterEntity;

  private final String projectEntity;

  private final String flowIdEntity;

  public AzkabanFlowUrn(String cluster, String project, String flowId) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, cluster, project, flowId));
    this.clusterEntity = cluster;
    this.projectEntity = project;
    this.flowIdEntity = flowId;
  }

  public String getClusterEntity() {
    return clusterEntity;
  }

  public String getProjectEntity() {
    return projectEntity;
  }

  public String getFlowIdEntity() {
    return flowIdEntity;
  }

  public static AzkabanFlowUrn createFromString(String rawUrn) throws URISyntaxException {
    String content = new Urn(rawUrn).getContent();
    String[] parts = content.substring(1, content.length() - 1).split(",");
    return new AzkabanFlowUrn(parts[0], parts[1], parts[2]);
  }

  public static AzkabanFlowUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(new DirectCoercer<AzkabanFlowUrn>() {
      public Object coerceInput(AzkabanFlowUrn object) throws ClassCastException {
        return object.toString();
      }

      public AzkabanFlowUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return AzkabanFlowUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, AzkabanFlowUrn.class);
  }
}
