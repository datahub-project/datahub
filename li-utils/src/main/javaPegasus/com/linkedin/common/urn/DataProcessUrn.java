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
  private final String _env;

  public DataProcessUrn(String orchestrator, String name, FabricType origin) {
    this(orchestrator, name, origin.name(), false);
  }

  /**
   * @param env a FabricType symbol, or the id of a registered customFabricType
   */
  public DataProcessUrn(String orchestrator, String name, String env) {
    this(orchestrator, name, env, true);
  }

  private DataProcessUrn(String orchestrator, String name, String env, boolean checkEnv) {
    super(
        ENTITY_TYPE,
        TupleKey.create(orchestrator, name, checkEnv ? FabricTypeUrnParts.checkEnv(env) : env));
    this._orchestrator = orchestrator;
    this._name = name;
    this._env = env;
  }

  public String getNameEntity() {
    return _name;
  }

  public String getOrchestratorEntity() {
    return _orchestrator;
  }

  /** The FabricType of this URN's env, or {@code $UNKNOWN} when the env is a custom value. */
  public FabricType getOriginEntity() {
    return FabricTypeUrnParts.origin(_env);
  }

  /** The env exactly as in the URN: a FabricType symbol or a custom value. */
  public String getEnv() {
    return _env;
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
              (String) key.getAs(2, String.class));
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
