package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class MLFeatureTableUrn extends Urn {

  public static final String ENTITY_TYPE = "mlFeatureTable";

  private final DataPlatformUrn _platform;
  private final String _name;

  public MLFeatureTableUrn(DataPlatformUrn platform, String name) {
    super(ENTITY_TYPE, TupleKey.create(platform, name));
    this._platform = platform;
    this._name = name;
  }

  public DataPlatformUrn getPlatformEntity() {
    return _platform;
  }

  public String getMlFeatureTableNameEntity() {
    return _name;
  }

  public static MLFeatureTableUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static MLFeatureTableUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'mlFeatureTable'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 2) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new MLFeatureTableUrn((DataPlatformUrn) key.getAs(0, DataPlatformUrn.class),
              (String) key.getAs(1, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static MLFeatureTableUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.initializeCustomClass(DataPlatformUrn.class);
    Custom.registerCoercer(new DirectCoercer<MLFeatureTableUrn>() {
      public Object coerceInput(MLFeatureTableUrn object) throws ClassCastException {
        return object.toString();
      }

      public MLFeatureTableUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return MLFeatureTableUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, MLFeatureTableUrn.class);
  }
}
