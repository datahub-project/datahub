package com.linkedin.common.urn;

import java.net.URISyntaxException;

import com.linkedin.common.FabricType;
import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

import static com.linkedin.common.urn.UrnUtils.toFabricType;


public final class MLModelUrn extends Urn {

  public static final String ENTITY_TYPE = "mlModel";

  private static final String CONTENT_FORMAT = "(%s,%s,%s)";

  private final DataPlatformUrn platformEntity;

  private final String mlModelNameEntity;

  private final FabricType originEntity;

  public MLModelUrn(DataPlatformUrn platform, String mlModelName, FabricType origin) {
    super(ENTITY_TYPE, String.format(CONTENT_FORMAT, platform.toString(), mlModelName, origin.name()));
    this.platformEntity = platform;
    this.mlModelNameEntity = mlModelName;
    this.originEntity = origin;
  }

  public DataPlatformUrn getPlatformEntity() {
    return platformEntity;
  }

  public String getMlModelNameEntity() {
    return mlModelNameEntity;
  }

  public FabricType getOriginEntity() {
    return originEntity;
  }

  public static MLModelUrn createFromString(String rawUrn) throws URISyntaxException {
    String content = new Urn(rawUrn).getContent();
    String[] parts = content.substring(1, content.length() - 1).split(",");
    return new MLModelUrn(DataPlatformUrn.createFromString(parts[0]), parts[1], toFabricType(parts[2]));
  }

  public static MLModelUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(new DirectCoercer<MLModelUrn>() {
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
    }, MLModelUrn.class);
  }
}
