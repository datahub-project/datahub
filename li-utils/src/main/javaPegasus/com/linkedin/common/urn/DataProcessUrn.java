package com.linkedin.common.urn;

import com.linkedin.common.FabricType;
import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public class DataProcessUrn extends Urn {
  public static final String ENTITY_TYPE = "dataProcess";

  private final String _name;
  private final String _orchestrator;
  private final FabricType _origin;

  public DataProcessUrn(String orchestrator, String name, FabricType origin) {
    super(ENTITY_TYPE, TupleKey.create(orchestrator, name, origin));
    this._orchestrator = orchestrator;
    this._name = name;
    this._origin = origin;
  }

  public String getNameEntity() {
    return _name;
  }

  public String getOrchestratorEntity() {
    return _orchestrator;
  }

  public FabricType getOriginEntity() {
    return _origin;
  }

  public static DataProcessUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static DataProcessUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  public static DataProcessUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'dataProcess'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 3) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new DataProcessUrn(
              (String) key.getAs(0, String.class),
              (String) key.getAs(1, String.class),
              (FabricType) key.getAs(2, FabricType.class));
        } catch (Exception var3) {
          throw new URISyntaxException(
              urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
        }
      }
    }
  }

  static {
    Custom.initializeCustomClass(DataProcessUrn.class);
    Custom.initializeCustomClass(FabricType.class);
    Custom.registerCoercer(
        new DirectCoercer<DataProcessUrn>() {
          public Object coerceInput(DataProcessUrn object) throws ClassCastException {
            return object.toString();
          }

          public DataProcessUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return DataProcessUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        DataProcessUrn.class);
  }
}
