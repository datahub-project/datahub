package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class AzkabanFlowUrn extends Urn {

  public static final String ENTITY_TYPE = "azkabanFlow";

  private final String _cluster;
  private final String _project;
  private final String _flowId;

  public AzkabanFlowUrn(String cluster, String project, String flowId) {
    super(ENTITY_TYPE, TupleKey.create(cluster, project, flowId));
    this._cluster = cluster;
    this._project = project;
    this._flowId = flowId;
  }

  public String getClusterEntity() {
    return _cluster;
  }

  public String getProjectEntity() {
    return _project;
  }

  public String getFlowIdEntity() {
    return _flowId;
  }

  public static AzkabanFlowUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static AzkabanFlowUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'azkabanFlow'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 3) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new AzkabanFlowUrn(
              (String) key.getAs(0, String.class),
              (String) key.getAs(1, String.class),
              (String) key.getAs(2, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static AzkabanFlowUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<AzkabanFlowUrn>() {
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
        },
        AzkabanFlowUrn.class);
  }
}
