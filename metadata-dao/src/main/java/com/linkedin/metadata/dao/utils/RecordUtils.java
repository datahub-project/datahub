package com.linkedin.metadata.dao.utils;

import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.validator.InvalidSchemaException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class RecordUtils {

  private static final JacksonDataTemplateCodec DATA_TEMPLATE_CODEC = new JacksonDataTemplateCodec();

  private RecordUtils() {
    // Util class
  }

  /**
   * Serializes a {@link RecordTemplate} to JSON string.
   *
   * @param recordTemplate the record template to serialize
   * @return the JSON string serialized using {@link JacksonDataTemplateCodec}.
   */
  @Nonnull
  public static String toJsonString(@Nonnull RecordTemplate recordTemplate) {
    try {
      return DATA_TEMPLATE_CODEC.mapToString(recordTemplate.data());
    } catch (IOException e) {
      throw new ModelConversionException("Failed to serialize RecordTemplate: " + recordTemplate.toString());
    }
  }

  /**
   * Creates a {@link RecordTemplate} object from a serialized JSON string
   *
   * @param type the type of {@link RecordTemplate} to create
   * @param jsonString a JSON string serialized using {@link JacksonDataTemplateCodec}
   * @param <T> must be a valid {@link RecordTemplate} class
   * @return the created {@link RecordTemplate}
   */
  @Nonnull
  public static <T extends RecordTemplate> T toRecordTemplate(@Nonnull Class<T> type, @Nonnull String jsonString) {
    DataMap dataMap;
    try {
      dataMap = DATA_TEMPLATE_CODEC.stringToMap(jsonString);
    } catch (IOException e) {
      throw new ModelConversionException("Failed to deserialize DataMap: " + jsonString);
    }

    return toRecordTemplate(type, dataMap);
  }

  /**
   * Creates a {@link RecordTemplate} object from a {@link DataMap}
   *
   * @param type the type of {@link RecordTemplate} to create
   * @param dataMap a {@link DataMap} of the record
   * @param <T> must be a valid {@link RecordTemplate} class
   * @return the created {@link RecordTemplate}
   */
  @Nonnull
  public static <T extends RecordTemplate> T toRecordTemplate(@Nonnull Class<T> type, @Nonnull DataMap dataMap) {
    Constructor<T> constructor;
    try {
      constructor = type.getConstructor(DataMap.class);
    } catch (NoSuchMethodException e) {
      throw new ModelConversionException("Unable to find constructor for " + type.getCanonicalName(), e);
    }

    try {
      return constructor.newInstance(dataMap);
    } catch (Exception e) {
      throw new ModelConversionException("Failed to invoke constructor for " + type.getCanonicalName(), e);
    }
  }

  /**
   * Creates a {@link RecordTemplate} object from class FQCN and a {@link DataMap}
   *
   * @param className FQCN of the record class extending RecordTemplate
   * @param dataMap a {@link DataMap} of the record
   * @return the created {@link RecordTemplate}
   */
  @Nonnull
  public static RecordTemplate toRecordTemplate(@Nonnull String className, @Nonnull DataMap dataMap) {
    Class<? extends RecordTemplate> clazz;
    try {
      clazz = Class.forName(className).asSubclass(RecordTemplate.class);
    } catch (ClassNotFoundException e) {
      throw new ModelConversionException("Unable to find class " + className, e);
    }

    return toRecordTemplate(clazz, dataMap);
  }

  /**
   * Extracts the aspect from an entity value which includes a single aspect.
   *
   * @param entity the entity value.
   * @param aspectClass the aspect class.
   * @return the aspect which is included in the entity.
   * */
  @Nonnull
  public static <ASPECT extends RecordTemplate, ENTITY extends RecordTemplate> ASPECT
  extractAspectFromSingleAspectEntity(@Nonnull ENTITY entity, @Nonnull Class<ASPECT> aspectClass) {

    // Create an empty aspect to extract it's field names
    final Constructor<ASPECT> constructor;
    try {
      @SuppressWarnings("rawtypes")
      final Class[] constructorParamArray = new Class[] {};
      constructor = aspectClass.getConstructor(constructorParamArray);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Exception occurred while trying to get the default constructor for the aspect. ", e);
    }

    final ASPECT aspect;
    try {
      aspect = constructor.newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Exception occurred while creating an instance of the aspect. ", e);
    }

    final Set<String> aspectFields = aspect.schema().getFields()
        .stream()
        .map(RecordDataSchema.Field::getName)
        .collect(Collectors.toSet());

    // Get entity's field names and only keep fields which occur in the entity and not in the aspect
    final Set<String> entityFields = entity.schema().getFields()
        .stream()
        .map(RecordDataSchema.Field::getName)
        .collect(Collectors.toSet());
    entityFields.removeAll(aspectFields);

    // remove non aspect fields from entity's cloned datamap and use it to create an aspect
    final DataMap entityDataMap;
    try {
      entityDataMap = entity.data().clone();
      entityFields.forEach(entityDataMap::remove);
      return toRecordTemplate(aspectClass, entityDataMap);
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets a {@link com.linkedin.data.schema.RecordDataSchema.Field} from a {@link RecordTemplate}.
   *
   * @param recordTemplate the {@link RecordTemplate} containing the field
   * @param fieldName the name of the field
   * @return the field
   */
  @Nonnull
  public static <T extends RecordTemplate> RecordDataSchema.Field getRecordDataSchemaField(@Nonnull T recordTemplate,
      @Nonnull String fieldName) {

    RecordDataSchema.Field field = recordTemplate.schema().getField(fieldName);
    if (field == null) {
      throw new InvalidSchemaException(
          String.format("Missing expected field '%s' in %s", fieldName, recordTemplate.getClass().getCanonicalName()));
    }
    return field;
  }

  /**
   * Sets the value of a primitive field in a {@link RecordTemplate} using reflection.
   *
   * @param recordTemplate the {@link RecordTemplate} containing the field
   * @param fieldName the name of the field to update
   * @param value the value to set
   */
  public static <T extends RecordTemplate, V> void setRecordTemplatePrimitiveField(@Nonnull T recordTemplate,
      @Nonnull String fieldName, @Nonnull V value) {

    final RecordDataSchema.Field field = getRecordDataSchemaField(recordTemplate, fieldName);
    final Method putDirect =
        getProtectedMethod(RecordTemplate.class, "putDirect", RecordDataSchema.Field.class, Class.class, Object.class,
            SetMode.class);
    invokeProtectedMethod(recordTemplate, putDirect, field, value.getClass(), value, SetMode.DISALLOW_NULL);
  }

  /**
   * Sets the value of a {@link RecordTemplate} field in a {@link RecordTemplate} using reflection.
   *
   * @param recordTemplate the {@link RecordTemplate} containing the field
   * @param fieldName the name of the field to update
   * @param value the value to set
   */
  public static <T extends RecordTemplate, V extends DataTemplate> void setRecordTemplateComplexField(
      @Nonnull T recordTemplate, @Nonnull String fieldName, @Nonnull V value) {

    final RecordDataSchema.Field field = getRecordDataSchemaField(recordTemplate, fieldName);
    final Method putWrapped =
        getProtectedMethod(RecordTemplate.class, "putWrapped", RecordDataSchema.Field.class, Class.class,
            DataTemplate.class, SetMode.class);
    invokeProtectedMethod(recordTemplate, putWrapped, field, value.getClass(), value, SetMode.DISALLOW_NULL);
  }

  /**
   * Gets the value of a non-wrapped field from a {@link RecordTemplate} using reflection.
   *
   * @param recordTemplate the {@link RecordTemplate} to get the value from
   * @param fieldName the name of the field
   * @param valueClass the expected type for the value
   * @return the value for the field
   */
  @Nonnull
  public static <T extends RecordTemplate, V> V getRecordTemplateField(@Nonnull T recordTemplate,
      @Nonnull String fieldName, @Nonnull Class<V> valueClass) {

    final RecordDataSchema.Field field = getRecordDataSchemaField(recordTemplate, fieldName);
    final Method obtainCustomType =
        getProtectedMethod(RecordTemplate.class, "obtainCustomType", RecordDataSchema.Field.class, Class.class,
            GetMode.class);
    return (V) invokeProtectedMethod(recordTemplate, obtainCustomType, field, valueClass, GetMode.STRICT);
  }

  /**
   * Gets the value of a wrapped field from a {@link RecordTemplate} using reflection.
   *
   * @param recordTemplate the {@link RecordTemplate} to get the value from
   * @param fieldName the name of the field
   * @param valueClass the expected type for the value
   * @return the value for the field
   */
  @Nonnull
  public static <T extends RecordTemplate, V extends DataTemplate> V getRecordTemplateWrappedField(
      @Nonnull T recordTemplate, @Nonnull String fieldName, @Nonnull Class<V> valueClass) {

    final RecordDataSchema.Field field = getRecordDataSchemaField(recordTemplate, fieldName);
    final Method obtainWrapped =
        getProtectedMethod(RecordTemplate.class, "obtainWrapped", RecordDataSchema.Field.class, Class.class,
            GetMode.class);
    return (V) invokeProtectedMethod(recordTemplate, obtainWrapped, field, valueClass, GetMode.STRICT);
  }

  /**
   * Gets the currently selected {@link RecordTemplate} in a {@link UnionTemplate}.
   *
   * @param unionTemplate the {@link UnionTemplate} to check
   * @return the currently selected value in {@code unionTemplate}
   */
  @Nonnull
  public static <V extends RecordTemplate> RecordTemplate getSelectedRecordTemplateFromUnion(
      @Nonnull UnionTemplate unionTemplate) {

    final DataSchema dataSchema = unionTemplate.memberType();
    if (!(dataSchema instanceof RecordDataSchema)) {
      throw new InvalidSchemaException(
          "The currently selected member isn't a RecordTemplate in " + unionTemplate.getClass().getCanonicalName());
    }

    final Class<? extends RecordTemplate> clazz =
        ModelUtils.getClassFromName(((RecordDataSchema) dataSchema).getBindingName(), RecordTemplate.class);

    final Method obtainWrapped =
        getProtectedMethod(UnionTemplate.class, "obtainWrapped", DataSchema.class, Class.class, String.class);
    return (V) invokeProtectedMethod(unionTemplate, obtainWrapped, dataSchema, clazz,
        ((RecordDataSchema) dataSchema).getFullName());
  }

  /**
   * Sets the currently selected {@link RecordTemplate} in a {@link UnionTemplate}.
   *
   * @param unionTemplate the {@link UnionTemplate} to select
   * @param selectedMember the {@link RecordTemplate} to set to
   * @return the currently selected value in {@code unionTemplate}
   */
  @Nonnull
  public static <V extends RecordTemplate> RecordTemplate setSelectedRecordTemplateInUnion(
      @Nonnull UnionTemplate unionTemplate, @Nonnull RecordTemplate selectedMember) {

    final Method selectWrapped =
        getProtectedMethod(UnionTemplate.class, "selectWrapped", DataSchema.class, Class.class, String.class,
            DataTemplate.class);
    return (V) invokeProtectedMethod(unionTemplate, selectWrapped, selectedMember.schema(), selectedMember.getClass(),
        selectedMember.schema().getUnionMemberKey(), selectedMember);
  }

  @Nonnull
  private static Method getProtectedMethod(@Nonnull Class clazz, @Nonnull String methodName,
      @Nonnull Class<?>... parameterTypes) {
    try {
      return clazz.getDeclaredMethod(methodName, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  private static <T> T invokeProtectedMethod(Object object, Method method, Object... args) {
    try {
      method.setAccessible(true);
      return (T) method.invoke(object, args);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    } finally {
      method.setAccessible(false);
    }
  }
}
