package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class AzkabanJobUrn extends Urn {

  public static final String ENTITY_TYPE = "azkabanJob";

  private final AzkabanFlowUrn _flow;
  private final String _jobId;

  public AzkabanJobUrn(AzkabanFlowUrn flow, String jobId) {
    super(ENTITY_TYPE, TupleKey.create(flow, jobId));
    this._flow = flow;
    this._jobId = jobId;
  }

  public AzkabanFlowUrn getFlowEntity() {
    return _flow;
  }

  public String getJobIdEntity() {
    return _jobId;
  }

  public static AzkabanJobUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static AzkabanJobUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'azkabanJob'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 2) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new AzkabanJobUrn(
              (AzkabanFlowUrn) key.getAs(0, AzkabanFlowUrn.class),
              (String) key.getAs(1, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static AzkabanJobUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.initializeCustomClass(AzkabanFlowUrn.class);
    Custom.registerCoercer(
        new DirectCoercer<AzkabanJobUrn>() {
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
        },
        AzkabanJobUrn.class);
  }
}
