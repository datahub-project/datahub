package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class DataPlatformUrn extends Urn {

  public static final String ENTITY_TYPE = "dataPlatform";

  private final String _platformName;

  public DataPlatformUrn(String platformName) {
    super(ENTITY_TYPE, TupleKey.create(platformName));
    this._platformName = platformName;
  }

  public String getPlatformNameEntity() {
    return _platformName;
  }

  public static DataPlatformUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static DataPlatformUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'dataPlatform'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new DataPlatformUrn((String) key.getAs(0, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static DataPlatformUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<DataPlatformUrn>() {
          public Object coerceInput(DataPlatformUrn object) throws ClassCastException {
            return object.toString();
          }

          public DataPlatformUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return DataPlatformUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        DataPlatformUrn.class);
  }
}
