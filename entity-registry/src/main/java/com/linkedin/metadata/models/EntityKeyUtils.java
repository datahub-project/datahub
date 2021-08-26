package com.linkedin.metadata.models;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import javax.annotation.Nonnull;


public class EntityKeyUtils {

  private EntityKeyUtils() {
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
}
