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
  private final FabricType _origin;

  public MlModelGroupUrn(DataPlatformUrn platform, String name, FabricType origin) {
    super(ENTITY_TYPE, TupleKey.create(platform, name, origin));
    this._platform = platform;
    this._name = name;
    this._origin = origin;
  }

  public DataPlatformUrn getPlatformEntity() {
    return _platform;
  }

  public String getNameEntity() {
    return _name;
  }

  public FabricType getOriginEntity() {
    return _origin;
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
              (FabricType) key.getAs(2, FabricType.class));
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
