package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class DataFlowUrn extends Urn {

  public static final String ENTITY_TYPE = "dataFlow";

  private final String _orchestrator;
  private final String _flowId;
  private final String _cluster;

  public DataFlowUrn(String orchestrator, String flowId, String cluster) {
    super(ENTITY_TYPE, TupleKey.create(orchestrator, flowId, cluster));
    this._orchestrator = orchestrator;
    this._flowId = flowId;
    this._cluster = cluster;
  }

  public String getOrchestratorEntity() {
    return _orchestrator;
  }

  public String getFlowIdEntity() {
    return _flowId;
  }

  public String getClusterEntity() {
    return _cluster;
  }

  public static DataFlowUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static DataFlowUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'dataFlow'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 3) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new DataFlowUrn(
              (String) key.getAs(0, String.class),
              (String) key.getAs(1, String.class),
              (String) key.getAs(2, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static DataFlowUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<DataFlowUrn>() {
          public Object coerceInput(DataFlowUrn object) throws ClassCastException {
            return object.toString();
          }

          public DataFlowUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return DataFlowUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        DataFlowUrn.class);
  }
}
