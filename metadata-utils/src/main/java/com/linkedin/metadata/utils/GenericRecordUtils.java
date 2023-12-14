package com.linkedin.metadata.utils;

import com.datahub.util.RecordUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.GenericPayload;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

public class GenericRecordUtils {
  public static final String JSON = "application/json";

  private GenericRecordUtils() {}

  /** Deserialize the given value into the aspect based on the input aspectSpec */
  @Nonnull
  public static RecordTemplate deserializeAspect(
      @Nonnull ByteString aspectValue,
      @Nonnull String contentType,
      @Nonnull AspectSpec aspectSpec) {
    return deserializeAspect(aspectValue, contentType, aspectSpec.getDataTemplateClass());
  }

  @Nonnull
  public static <T extends RecordTemplate> T deserializeAspect(
      @Nonnull ByteString aspectValue, @Nonnull String contentType, @Nonnull Class<T> clazz) {
    if (!contentType.equals(JSON)) {
      throw new IllegalArgumentException(
          String.format("%s content type is not supported", contentType));
    }
    return RecordUtils.toRecordTemplate(clazz, aspectValue.asString(StandardCharsets.UTF_8));
  }

  @Nonnull
  public static <T extends RecordTemplate> T deserializePayload(
      @Nonnull ByteString payloadValue, @Nonnull String contentType, @Nonnull Class<T> clazz) {
    if (!contentType.equals(JSON)) {
      throw new IllegalArgumentException(
          String.format("%s content type is not supported", contentType));
    }
    return RecordUtils.toRecordTemplate(clazz, payloadValue.asString(StandardCharsets.UTF_8));
  }

  @Nonnull
  public static <T extends RecordTemplate> T deserializePayload(
      @Nonnull ByteString payloadValue, @Nonnull Class<T> clazz) {
    return deserializePayload(payloadValue, JSON, clazz);
  }

  @Nonnull
  public static GenericAspect serializeAspect(@Nonnull RecordTemplate aspect) {
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(
        ByteString.unsafeWrap(RecordUtils.toJsonString(aspect).getBytes(StandardCharsets.UTF_8)));
    genericAspect.setContentType(GenericRecordUtils.JSON);
    return genericAspect;
  }

  @Nonnull
  public static GenericPayload serializePayload(@Nonnull RecordTemplate payload) {
    GenericPayload genericPayload = new GenericPayload();
    genericPayload.setValue(
        ByteString.unsafeWrap(RecordUtils.toJsonString(payload).getBytes(StandardCharsets.UTF_8)));
    genericPayload.setContentType(GenericRecordUtils.JSON);
    return genericPayload;
  }
}
