package com.linkedin.metadata.util;

import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.models.AspectSpec;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;


public class AspectDeserializationUtil {
  public static final String JSON = "application/json";

  private AspectDeserializationUtil() {
  }

  /**
   * Deserialize the given value into the aspect based on the input aspectSpec
   */
  @Nonnull
  public static RecordTemplate deserializeAspect(@Nonnull ByteString aspectValue, @Nonnull String contentType,
      @Nonnull AspectSpec aspectSpec) {
    if (!contentType.equals(JSON)) {
      throw new IllegalArgumentException(String.format("%s content type is not supported", contentType));
    }
    return RecordUtils.toRecordTemplate(
        PegasusUtils.getDataTemplateClassFromSchema(aspectSpec.getPegasusSchema(), RecordTemplate.class),
        aspectValue.asString(StandardCharsets.UTF_8));
  }
}
