package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public final class AzkabanJobUrn extends Urn {

  public static final String ENTITY_TYPE = "azkabanJob";

  private static final String CONTENT_FORMAT = "(%s,%s)";

  private final AzkabanFlowUrn flowEntity;

  private final String jobIdEntity;

  public AzkabanJobUrn(AzkabanFlowUrn flow, String jobId) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, flow.toString(), jobId));
    this.flowEntity = flow;
    this.jobIdEntity = jobId;
  }

  public AzkabanFlowUrn getFlowEntity() {
    return flowEntity;
  }

  public String getJobIdEntity() {
    return jobIdEntity;
  }

  public static AzkabanJobUrn createFromString(String rawUrn) throws URISyntaxException {
    String content = new Urn(rawUrn).getContent();
    String flowParts = content.substring(1, content.lastIndexOf(",") + 1);
    String[] parts = content.substring(1, content.length() - 1).split(",");
    return new AzkabanJobUrn(AzkabanFlowUrn.createFromString(flowParts), parts[3]);
  }

  public static AzkabanJobUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(new DirectCoercer<AzkabanJobUrn>() {
      public Object coerceInput(AzkabanJobUrn object) throws ClassCastException {
        return object.toString();
      }

      public AzkabanJobUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return AzkabanJobUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, AzkabanJobUrn.class);
  }
}
