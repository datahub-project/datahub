package com.linkedin.gms.factory.common;

import com.google.common.reflect.TypeToken;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.storage.LocalDAOStorageConfig;
import com.linkedin.metadata.validator.AspectValidator;
import com.linkedin.metadata.validator.ValidationUtils;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Class containing utility methods for {@link LocalDAOStorageConfig} config.
 */
public final class LocalDAOStorageConfigFactory {

  private LocalDAOStorageConfigFactory() { }

  /**
   * Gets {@link LocalDAOStorageConfig} containing aspects in aspect union. All aspects are populated with NULL
   * (default) config.
   *
   * @param aspectUnionClass class of the aspect union from where the set of aspects are retrieved
   * @return {@link LocalDAOStorageConfig} that contains set of aspects in the aspect union, each with NULL (default) config
   */
  @Nonnull
  public static LocalDAOStorageConfig getStorageConfig(@Nonnull Class<? extends UnionTemplate> aspectUnionClass) {
    AspectValidator.validateAspectUnionSchema(aspectUnionClass);
    final Set<Class<? extends RecordTemplate>> aspects = ModelUtils.getValidAspectTypes(aspectUnionClass);
    return LocalDAOStorageConfig.builder().aspectStorageConfigMap(getDefaultMapForAspects(aspects)).build();
  }

  /**
   * Gets {@link LocalDAOStorageConfig} containing merged storage config.
   *
   * <p>The config provided in input resource file is merged with the aspects in aspect union not covered in the input
   * JSON, to return a complete view of the set of aspects along with their storage config. NULL (default) config is
   * chosen for all aspects not covered in the input JSON. Contents of the input resource file, represented as JSON,
   * should match with the schema of {@link LocalDAOStorageConfig}.
   *
   * @param aspectUnionClass class of the aspect union from where the complete set of aspects is retrieved
   * @param resourceClass class corresponding to the resource file containing the storage config as JSON
   * @param filePath path of the resource file containing the storage config as JSON
   * @return {@link LocalDAOStorageConfig} containing merged set of aspects along with their storage config
   */
  @Nonnull
  public static LocalDAOStorageConfig getStorageConfig(@Nonnull Class<? extends UnionTemplate> aspectUnionClass,
      @Nonnull Class resourceClass, @Nonnull String filePath) {

    AspectValidator.validateAspectUnionSchema(aspectUnionClass);
    final Set<Class<? extends RecordTemplate>> aspects = ModelUtils.getValidAspectTypes(aspectUnionClass);
    final LocalDAOStorageConfig storageConfigInfo = readResourceFile(resourceClass, filePath);
    final Set<Class<? extends RecordTemplate>> configAspects = storageConfigInfo.getAspectStorageConfigMap().keySet();

    // validate that aspects in the config file are part of the aspect union
    storageConfigInfo.getAspectStorageConfigMap().keySet()
        .forEach(x -> AspectValidator.validateAspectUnionSchema(ValidationUtils.getUnionSchema(aspectUnionClass), x.getCanonicalName()));

    aspects.removeAll(configAspects);
    storageConfigInfo.getAspectStorageConfigMap().putAll(getDefaultMapForAspects(aspects));
    return storageConfigInfo;
  }

  /**
   * Gets {@link LocalDAOStorageConfig} containing the set of aspects along with their storage config, all of which are
   * provided in the input resource file. Contents of the input resource file, represented as JSON, should match with
   * the schema of {@link LocalDAOStorageConfig}.
   *
   * @param resourceClass class corresponding to the resource file containing the storage config as JSON
   * @param filePath path of the resource file containing the storage config as JSON
   * @return {@link LocalDAOStorageConfig} containing set of aspects along with their storage config
   */
  @Nonnull
  public static LocalDAOStorageConfig getStorageConfig(@Nonnull Class resourceClass, @Nonnull String filePath) {
    return readResourceFile(resourceClass, filePath);
  }

  /**
   * TODO: validate path spec of relevant aspects in input resource file.
   */
  @Nonnull
  static LocalDAOStorageConfig readResourceFile(@Nonnull Class resourceClass, @Nonnull String filePath) {
    try (
        final InputStream inputStream = resourceClass.getClassLoader().getResourceAsStream(filePath);
        final InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF-8")
    ) {
      final GsonBuilder gsonBuilder = new GsonBuilder();
      gsonBuilder.registerTypeAdapter(LocalDAOStorageConfig.class, new LocalDAOStorageConfigCustomDeserializer());
      return gsonBuilder.create().fromJson(inputStreamReader, LocalDAOStorageConfig.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  private static Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> getDefaultMapForAspects(
      @Nonnull Set<Class<? extends RecordTemplate>> aspects) {
    final Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> aspectConfig = new HashMap<>();
    for (final Class<? extends RecordTemplate> aspect : aspects) {
      aspectConfig.put(aspect, null);
    }
    return aspectConfig;
  }

  private final static class LocalDAOStorageConfigCustomDeserializer implements JsonDeserializer<LocalDAOStorageConfig> {

    private static final String KEY = "aspectStorageConfigMap";
    private static final Type TYPE_TOKEN = new TypeToken<Map<String, LocalDAOStorageConfig.AspectStorageConfig>>() { }.getType();

    @Nonnull
    @Override
    public LocalDAOStorageConfig deserialize(@Nonnull final JsonElement jElement, @Nonnull final Type typeOfT,
        @Nonnull final JsonDeserializationContext context) throws JsonParseException {
      final JsonObject jObject = jElement.getAsJsonObject();
      final Map<Class<? extends RecordTemplate>, LocalDAOStorageConfig.AspectStorageConfig> aspectStorageConfigMap = new HashMap<>();
      if (jObject.has(KEY)) {
        final Map<String, LocalDAOStorageConfig.AspectStorageConfig> origMap = context.deserialize(jObject.get(KEY), TYPE_TOKEN);
        for (final Map.Entry<String, LocalDAOStorageConfig.AspectStorageConfig> entry : origMap.entrySet()) {
          aspectStorageConfigMap.put(ModelUtils.getAspectClass(entry.getKey()), entry.getValue());
        }
      }
      return LocalDAOStorageConfig.builder().aspectStorageConfigMap(aspectStorageConfigMap).build();
    }
  }

}