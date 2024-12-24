package com.linkedin.metadata.utils;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.EnvelopedSystemAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.GenericPayload;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class GenericRecordUtils {
  public static final String JSON = "application/json";

  private GenericRecordUtils() {}

  public static <T extends RecordTemplate> T copy(T input, Class<T> clazz) {
    try {
      if (input == null) {
        return null;
      }
      Constructor<T> constructor = clazz.getConstructor(DataMap.class);
      return constructor.newInstance(input.data().copy());
    } catch (CloneNotSupportedException
        | InvocationTargetException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

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
    return serializeAspect(RecordUtils.toJsonString(aspect));
  }

  @Nonnull
  public static GenericAspect serializeAspect(@Nonnull String str) {
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(str.getBytes(StandardCharsets.UTF_8)));
    genericAspect.setContentType(GenericRecordUtils.JSON);
    return genericAspect;
  }

  @Nonnull
  public static GenericAspect serializeAspect(@Nonnull JsonNode json) {
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(ByteString.unsafeWrap(json.toString().getBytes(StandardCharsets.UTF_8)));
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

  @Nonnull
  public static Map<Urn, Map<String, Aspect>> entityResponseToAspectMap(
      Map<Urn, EntityResponse> inputMap) {
    return inputMap.entrySet().stream()
        .map(
            entry ->
                Map.entry(
                    entry.getKey(),
                    entry.getValue().getAspects().entrySet().stream()
                        .map(
                            aspectEntry ->
                                Map.entry(aspectEntry.getKey(), aspectEntry.getValue().getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Nonnull
  public static Map<Urn, Map<String, SystemAspect>> entityResponseToSystemAspectMap(
      Map<Urn, EntityResponse> inputMap, @Nonnull EntityRegistry entityRegistry) {
    return inputMap.entrySet().stream()
        .map(
            entry ->
                Map.entry(
                    entry.getKey(),
                    entry.getValue().getAspects().entrySet().stream()
                        .filter(aspectEntry -> aspectEntry.getValue() != null)
                        .map(
                            aspectEntry ->
                                Map.entry(
                                    aspectEntry.getKey(),
                                    EnvelopedSystemAspect.of(
                                        entry.getKey(),
                                        aspectEntry.getValue(),
                                        entityRegistry.getEntitySpec(
                                            entry.getKey().getEntityType()))))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
