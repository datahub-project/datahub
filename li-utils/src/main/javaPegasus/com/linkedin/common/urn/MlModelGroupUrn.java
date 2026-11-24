package com.linkedin.common.urn;

import com.linkedin.common.FabricType;
import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class MlModelGroupUrn extends Urn {

  public static final String ENTITY_TYPE = "mlModelGroup";

  private final DataPlatformUrn _platform;
  private final String _name;
  private final String _env;

  public MlModelGroupUrn(DataPlatformUrn platform, String name, FabricType origin) {
    this(platform, name, origin.name(), false);
  }

  /**
   * @param env a FabricType symbol, or the id of a registered customFabricType
   */
  public MlModelGroupUrn(DataPlatformUrn platform, String name, String env) {
    this(platform, name, env, true);
  }

  private MlModelGroupUrn(DataPlatformUrn platform, String name, String env, boolean checkEnv) {
    super(
        ENTITY_TYPE,
        TupleKey.create(platform, name, checkEnv ? FabricTypeUrnParts.checkEnv(env) : env));
    this._platform = platform;
    this._name = name;
    this._env = env;
  }

  public DataPlatformUrn getPlatformEntity() {
    return _platform;
  }

  public String getNameEntity() {
    return _name;
  }

  /** The FabricType of this URN's env, or {@code $UNKNOWN} when the env is a custom value. */
  public FabricType getOriginEntity() {
    return FabricTypeUrnParts.origin(_env);
  }

  /** The env exactly as in the URN: a FabricType symbol or a custom value. */
  public String getEnv() {
    return _env;
  }

  public static MlModelGroupUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static MlModelGroupUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'mlModelGroup'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 3) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new MlModelGroupUrn(
              (DataPlatformUrn) key.getAs(0, DataPlatformUrn.class),
              (String) key.getAs(1, String.class),
              (String) key.getAs(2, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static MlModelGroupUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.initializeCustomClass(DataPlatformUrn.class);
    Custom.initializeCustomClass(FabricType.class);
    Custom.registerCoercer(
        new DirectCoercer<MlModelGroupUrn>() {
          public Object coerceInput(MlModelGroupUrn object) throws ClassCastException {
            return object.toString();
          }

          public MlModelGroupUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return MlModelGroupUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        MlModelGroupUrn.class);
  }
}
