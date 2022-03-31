package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class EntityKeyUtils {

  private EntityKeyUtils() {
  }

  @Nonnull
  public static Urn getUrnFromProposal(MetadataChangeProposal metadataChangeProposal, AspectSpec keyAspectSpec) {

    if (metadataChangeProposal.hasEntityUrn()) {
      Urn urn = metadataChangeProposal.getEntityUrn();
      // Validate Urn
      try {
        EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec.getPegasusSchema());
      } catch (RuntimeException re) {
        throw new RuntimeException(String.format("Failed to validate entity URN %s", urn), re);
      }
      return urn;
    }
    if (metadataChangeProposal.hasEntityKeyAspect()) {
      RecordTemplate keyAspectRecord = GenericRecordUtils.deserializeAspect(
              metadataChangeProposal.getEntityKeyAspect().getValue(),
              metadataChangeProposal.getEntityKeyAspect().getContentType(),
              keyAspectSpec);
      return EntityKeyUtils.convertEntityKeyToUrn(keyAspectRecord, metadataChangeProposal.getEntityType());
    }
    throw new IllegalArgumentException("One of urn and keyAspect must be set");
  }

  @Nonnull
  public static Urn getUrnFromLog(MetadataChangeLog metadataChangeLog, AspectSpec keyAspectSpec) {
    if (metadataChangeLog.hasEntityUrn()) {
      Urn urn = metadataChangeLog.getEntityUrn();
      // Validate Urn
      try {
        EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec.getPegasusSchema());
      } catch (RuntimeException re) {
        throw new RuntimeException(String.format("Failed to validate entity URN %s", urn), re);
      }
      return urn;
    }
    if (metadataChangeLog.hasEntityKeyAspect()) {
      RecordTemplate keyAspectRecord = GenericRecordUtils.deserializeAspect(
          metadataChangeLog.getEntityKeyAspect().getValue(),
          metadataChangeLog.getEntityKeyAspect().getContentType(),
          keyAspectSpec);
      return EntityKeyUtils.convertEntityKeyToUrn(keyAspectRecord, metadataChangeLog.getEntityType());
    }
    throw new IllegalArgumentException("One of urn and keyAspect must be set");
  }

  /**
   * Implicitly converts a normal {@link Urn} into a {@link RecordTemplate} Entity Key given
   * the urn & the {@link RecordDataSchema} of the key.
   *
   * Parts of the urn are bound into fields in the keySchema based on field <b>index</b>. If the
   * number of urn key parts does not match the number of fields in the key schema, an {@link IllegalArgumentException} will be thrown.
   *
   * @param urn raw entity urn
   * @param keySchema schema of the entity key
   * @return a {@link RecordTemplate} created by mapping the fields of the urn to fields of
   * the provided key schema in order.
   * @throws {@link IllegalArgumentException} if the urn cannot be converted into the key schema (field number or type mismatch)
   */
  @Nonnull
  public static RecordTemplate convertUrnToEntityKey(@Nonnull final Urn urn, @Nonnull final RecordDataSchema keySchema) {

    // #1. Ensure we have a class to bind into.
    Class<? extends RecordTemplate> clazz;
    try {
      clazz = Class.forName(keySchema.getFullName()).asSubclass(RecordTemplate.class);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("Failed to find RecordTemplate class associated with provided RecordDataSchema named %s",
              keySchema.getFullName()), e);
    }

    // #2. Bind fields into a DataMap
    if (urn.getEntityKey().getParts().size() != keySchema.getFields().size()) {
      throw new IllegalArgumentException(
          "Failed to convert urn to entity key: urns parts and key fields do not have same length");
    }
    final DataMap dataMap = new DataMap();
    for (int i = 0; i < urn.getEntityKey().getParts().size(); i++) {
      final String urnPart = urn.getEntityKey().get(i);
      final RecordDataSchema.Field field = keySchema.getFields().get(i);
      dataMap.put(field.getName(), urnPart);
    }

    // #3. Finally, instantiate the record template with the newly created DataMap.
    Constructor<? extends RecordTemplate> constructor;
    try {
      constructor = clazz.getConstructor(DataMap.class);
      return constructor.newInstance(dataMap);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException(
          String.format("Failed to instantiate RecordTemplate with name %s. Missing constructor taking DataMap as arg.",
              clazz.getName()));
    }
  }

  /**
   * Implicitly converts an Entity Key {@link RecordTemplate} into the corresponding {@link Urn} string.
   *
   * Parts of the key record are bound into fields in the urn based on field <b>index</b>.
   *
   * @param keyAspect a {@link RecordTemplate} representing the key.
   * @param entityName name of the entity to use during Urn construction
   * @return an {@link Urn} created by binding the fields of the key aspect to an Urn.
   */
  @Nonnull
  public static Urn convertEntityKeyToUrn(@Nonnull final RecordTemplate keyAspect, @Nonnull final String entityName) {
    final List<String> urnParts = new ArrayList<>();
    for (RecordDataSchema.Field field : keyAspect.schema().getFields()) {
      Object value = keyAspect.data().get(field.getName());
      String valueString = value == null ? "" : value.toString();
      urnParts.add(valueString); // TODO: Determine whether all fields, including urns, should be URL encoded.
    }
    return Urn.createFromTuple(entityName, urnParts);
  }
}
