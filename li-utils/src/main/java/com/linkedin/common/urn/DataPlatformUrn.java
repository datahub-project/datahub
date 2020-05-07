package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

import java.net.URISyntaxException;


public final class DataPlatformUrn extends Urn {

  public static final String ENTITY_TYPE = "dataPlatform";

  private final String platformNameEntity;

  public DataPlatformUrn(String platformName) {
    super(ENTITY_TYPE, platformName);
    this.platformNameEntity = platformName;
  }

  public String getPlatformNameEntity() {
    return platformNameEntity;
  }

  public static DataPlatformUrn createFromString(String rawUrn) throws URISyntaxException {
    String platformName = new Urn(rawUrn).getContent();
    return new DataPlatformUrn(platformName);
  }

  public static DataPlatformUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(new DirectCoercer<DataPlatformUrn>() {
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
    }, DataPlatformUrn.class);
  }
}
