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
  private final FabricType _origin;

  public MLModelUrn(DataPlatformUrn platform, String mlModelName, FabricType origin) {
    super(ENTITY_TYPE, TupleKey.create(platform, mlModelName, origin));
    this._platform = platform;
    this._mlModelName = mlModelName;
    this._origin = origin;
  }

  public DataPlatformUrn getPlatformEntity() {
    return _platform;
  }

  public String getMlModelNameEntity() {
    return _mlModelName;
  }

  public FabricType getOriginEntity() {
    return _origin;
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
              (FabricType) key.getAs(2, FabricType.class));
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
