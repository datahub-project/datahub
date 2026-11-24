package com.linkedin.common.urn;

import com.linkedin.common.FabricType;
import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class MLModelUrn extends Urn {

  public static final String ENTITY_TYPE = "mlModel";

  private final DataPlatformUrn _platform;
  private final String _mlModelName;
  private final String _env;

  public MLModelUrn(DataPlatformUrn platform, String mlModelName, FabricType origin) {
    this(platform, mlModelName, origin.name(), false);
  }

  /**
   * @param env a FabricType symbol, or the id of a registered customFabricType
   */
  public MLModelUrn(DataPlatformUrn platform, String mlModelName, String env) {
    this(platform, mlModelName, env, true);
  }

  private MLModelUrn(DataPlatformUrn platform, String mlModelName, String env, boolean checkEnv) {
    super(
        ENTITY_TYPE,
        TupleKey.create(platform, mlModelName, checkEnv ? FabricTypeUrnParts.checkEnv(env) : env));
    this._platform = platform;
    this._mlModelName = mlModelName;
    this._env = env;
  }

  public DataPlatformUrn getPlatformEntity() {
    return _platform;
  }

  public String getMlModelNameEntity() {
    return _mlModelName;
  }

  /** The FabricType of this URN's env, or {@code $UNKNOWN} when the env is a custom value. */
  public FabricType getOriginEntity() {
    return FabricTypeUrnParts.origin(_env);
  }

  /** The env exactly as in the URN: a FabricType symbol or a custom value. */
  public String getEnv() {
    return _env;
  }

  public static MLModelUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static MLModelUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'mlModel'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 3) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new MLModelUrn(
              (DataPlatformUrn) key.getAs(0, DataPlatformUrn.class),
              (String) key.getAs(1, String.class),
              (String) key.getAs(2, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static MLModelUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.initializeCustomClass(DataPlatformUrn.class);
    Custom.initializeCustomClass(FabricType.class);
    Custom.registerCoercer(
        new DirectCoercer<MLModelUrn>() {
          public Object coerceInput(MLModelUrn object) throws ClassCastException {
            return object.toString();
          }

          public MLModelUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return MLModelUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        MLModelUrn.class);
  }
}
