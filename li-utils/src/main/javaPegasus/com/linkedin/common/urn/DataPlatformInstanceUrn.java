package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class DataPlatformInstanceUrn extends Urn {

  public static final String ENTITY_TYPE = "dataPlatformInstance";

  private final DataPlatformUrn _platform;
  private final String _instanceId;

  public DataPlatformInstanceUrn(DataPlatformUrn platform, String instanceId) {
    super(ENTITY_TYPE, TupleKey.create(platform, instanceId));
    this._platform = platform;
    this._instanceId = instanceId;
  }

  public DataPlatformUrn getPlatformEntity() {
    return _platform;
  }

  public String getInstance() {
    return _instanceId;
  }

  public static DataPlatformInstanceUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static DataPlatformInstanceUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(
          urn.toString(), "Urn entity type should be 'dataPlatformInstance'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 2) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new DataPlatformInstanceUrn(
              (DataPlatformUrn) key.getAs(0, DataPlatformUrn.class),
              (String) key.getAs(1, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static DataPlatformInstanceUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.initializeCustomClass(DataPlatformUrn.class);
    Custom.initializeCustomClass(DataPlatformInstanceUrn.class);
    Custom.registerCoercer(
        new DirectCoercer<DataPlatformInstanceUrn>() {
          public Object coerceInput(DataPlatformInstanceUrn object) throws ClassCastException {
            return object.toString();
          }

          public DataPlatformInstanceUrn coerceOutput(Object object)
              throws TemplateOutputCastException {
            try {
              return DataPlatformInstanceUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        DataPlatformInstanceUrn.class);
  }
}
