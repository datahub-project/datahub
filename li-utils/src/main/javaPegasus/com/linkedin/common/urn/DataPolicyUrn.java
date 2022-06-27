package com.linkedin.common.urn;

import com.linkedin.common.FabricType;
import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public final class DataPolicyUrn extends Urn {

  public static final String ENTITY_TYPE = "dataPolicy";

  private final DataPlatformUrn _platform;
  private final String _dataPolicyName;
  private final FabricType _origin;

  public DataPolicyUrn(DataPlatformUrn platform, String name, FabricType origin) {
    super(ENTITY_TYPE, TupleKey.create(platform, name, origin));
    this._platform = platform;
    this._dataPolicyName = name;
    this._origin = origin;
  }

  public DataPlatformUrn getPlatformEntity() {
    return _platform;
  }

  public String getDataPolicyNameEntity() {
    return _dataPolicyName;
  }

  public FabricType getOriginEntity() {
    return _origin;
  }

  public static DataPolicyUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static DataPolicyUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'dataset'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 3) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return new DataPolicyUrn((DataPlatformUrn) key.getAs(0, DataPlatformUrn.class),
              (String) key.getAs(1, String.class), (FabricType) key.getAs(2, FabricType.class));
        } catch (Exception var3) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
        }
      }
    }
  }

  public static DataPolicyUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.initializeCustomClass(DataPlatformUrn.class);
    Custom.initializeCustomClass(DataPolicyUrn.class);
    Custom.initializeCustomClass(FabricType.class);
    Custom.registerCoercer(new DirectCoercer<DataPolicyUrn>() {
      public Object coerceInput(DataPolicyUrn object) throws ClassCastException {
        return object.toString();
      }

      public DataPolicyUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return DataPolicyUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, DataPolicyUrn.class);
  }
}
