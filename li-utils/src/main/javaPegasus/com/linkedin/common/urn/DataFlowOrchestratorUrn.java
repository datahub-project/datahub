package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

import java.net.URISyntaxException;


public final class DataFlowOrchestratorUrn extends Urn {

  public static final String ENTITY_TYPE = "dataFlowOrchestrator";

  private final String _orchestratorName;

  public DataFlowOrchestratorUrn(String orchestratorName) {
    super(ENTITY_TYPE, TupleKey.create(orchestratorName));
    this._orchestratorName = orchestratorName;
  }

  public String getOrchestratorNameEntity() {
    return _orchestratorName;
  }

  public static DataFlowOrchestratorUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static DataFlowOrchestratorUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'dataFlowOrchestrator'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new DataFlowOrchestratorUrn((String) key.getAs(0, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static DataFlowOrchestratorUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(new DirectCoercer<DataFlowOrchestratorUrn>() {
      public Object coerceInput(DataFlowOrchestratorUrn object) throws ClassCastException {
        return object.toString();
      }

      public DataFlowOrchestratorUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return DataFlowOrchestratorUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, DataFlowOrchestratorUrn.class);
  }
}
