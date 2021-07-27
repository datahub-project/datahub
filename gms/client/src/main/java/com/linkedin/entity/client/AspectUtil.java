package com.linkedin.entity.client;

import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.utils.RecordUtils;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;


public class AspectUtil {

  public static final String JSON = "application/json";

  private AspectUtil() {
  }

  // TODO: Consolidate with AspectDeserializationUtil.
  @Nonnull
  public static <T extends RecordTemplate> T deserializeAspect(
      @Nonnull ByteString aspectValue,
      @Nonnull String contentType,
      @Nonnull Class<T> clazz) {
    if (!contentType.equals(JSON)) {
      throw new IllegalArgumentException(String.format("%s content type is not supported", contentType));
    }
    return RecordUtils.toRecordTemplate(
        clazz,
        aspectValue.asString(StandardCharsets.UTF_8));
  }
}
