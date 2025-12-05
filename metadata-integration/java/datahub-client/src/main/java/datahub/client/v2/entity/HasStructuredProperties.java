package datahub.client.v2.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertiesPatchBuilder;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Mixin interface providing structured property operations for entities that support structured
 * properties.
 *
 * <p>Entities implementing this interface can have typed key-value properties attached for
 * extensible metadata.
 *
 * <p>Example usage:
 *
 * <pre>
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_table")
 *     .build();
 *
 * // Set single values (type automatically detected)
 * dataset.setStructuredProperty("io.acryl.dataManagement.replicationSLA", "24h");
 * dataset.setStructuredProperty("io.acryl.dataQuality.qualityScore", 95.5);
 *
 * // Set multiple values
 * dataset.setStructuredProperty("io.acryl.dataManagement.certifications",
 *     Arrays.asList("SOC2", "HIPAA", "GDPR"));
 * dataset.setStructuredProperty("io.acryl.privacy.retentionDays",
 *     Arrays.asList(90, 180, 365));
 *
 * // Remove properties
 * dataset.removeStructuredProperty("io.acryl.dataManagement.deprecated");
 * </pre>
 *
 * @param <T> the concrete entity type (self-bounded generic for fluent returns)
 */
public interface HasStructuredProperties<T extends Entity & HasStructuredProperties<T>> {

  /**
   * Sets a single string value for a structured property.
   *
   * @param propertyUrn the structured property identifier, which can be either:
   *     <ul>
   *       <li>A qualified property name: {@code "io.acryl.dataManagement.replicationSLA"}
   *       <li>A full URN: {@code
   *           "urn:li:structuredProperty:io.acryl.dataManagement.replicationSLA"}
   *     </ul>
   *
   * @param value the string value to set
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setStructuredProperty(@Nonnull String propertyUrn, @Nonnull String value) {
    return setStructuredProperty(propertyUrn, Arrays.asList(value));
  }

  /**
   * Sets multiple string values for a structured property.
   *
   * @param propertyUrn the structured property identifier, which can be either:
   *     <ul>
   *       <li>A qualified property name: {@code "io.acryl.dataManagement.certifications"}
   *       <li>A full URN: {@code
   *           "urn:li:structuredProperty:io.acryl.dataManagement.certifications"}
   *     </ul>
   *
   * @param values the list of string values to set
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setStructuredProperty(@Nonnull String propertyUrn, @Nonnull List<String> values) {
    Urn urn = makeStructuredPropertyUrn(propertyUrn);
    Entity entity = (Entity) this;

    StructuredPropertiesPatchBuilder builder =
        entity.getPatchBuilder("structuredProperties", StructuredPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new StructuredPropertiesPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("structuredProperties", builder);
    }

    builder.setStringProperty(urn, values);
    return (T) this;
  }

  /**
   * Sets a single number value for a structured property.
   *
   * @param propertyUrn the structured property identifier, which can be either:
   *     <ul>
   *       <li>A qualified property name: {@code "io.acryl.dataQuality.qualityScore"}
   *       <li>A full URN: {@code "urn:li:structuredProperty:io.acryl.dataQuality.qualityScore"}
   *     </ul>
   *
   * @param value the number value to set
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setStructuredProperty(@Nonnull String propertyUrn, @Nonnull Number value) {
    Urn urn = makeStructuredPropertyUrn(propertyUrn);
    Entity entity = (Entity) this;

    StructuredPropertiesPatchBuilder builder =
        entity.getPatchBuilder("structuredProperties", StructuredPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new StructuredPropertiesPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("structuredProperties", builder);
    }

    builder.setNumberProperty(urn, value.intValue());
    return (T) this;
  }

  /**
   * Sets multiple number values for a structured property.
   *
   * <p>Note: To set multiple numbers, you need to use a typed list. Example: {@code
   * Arrays.asList(90, 180, 365)} works, but for mixed types use explicit casting.
   *
   * @param propertyUrn the structured property identifier, which can be either:
   *     <ul>
   *       <li>A qualified property name: {@code "io.acryl.privacy.retentionDays"}
   *       <li>A full URN: {@code "urn:li:structuredProperty:io.acryl.privacy.retentionDays"}
   *     </ul>
   *
   * @param value1 the first number value
   * @param value2 the second number value
   * @param additionalValues additional number values (varargs)
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setStructuredProperty(
      @Nonnull String propertyUrn,
      @Nonnull Number value1,
      @Nonnull Number value2,
      @Nonnull Number... additionalValues) {
    Urn urn = makeStructuredPropertyUrn(propertyUrn);
    Entity entity = (Entity) this;

    StructuredPropertiesPatchBuilder builder =
        entity.getPatchBuilder("structuredProperties", StructuredPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new StructuredPropertiesPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("structuredProperties", builder);
    }

    // Build list with all values
    List<Integer> intValues = new java.util.ArrayList<>();
    intValues.add(value1.intValue());
    intValues.add(value2.intValue());
    for (Number num : additionalValues) {
      intValues.add(num.intValue());
    }

    builder.setNumberProperty(urn, intValues);
    return (T) this;
  }

  /**
   * Removes a structured property from this entity.
   *
   * @param propertyUrn the structured property identifier to remove, which can be either:
   *     <ul>
   *       <li>A qualified property name: {@code "io.acryl.dataManagement.deprecated"}
   *       <li>A full URN: {@code "urn:li:structuredProperty:io.acryl.dataManagement.deprecated"}
   *     </ul>
   *
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T removeStructuredProperty(@Nonnull String propertyUrn) {
    Urn urn = makeStructuredPropertyUrn(propertyUrn);
    Entity entity = (Entity) this;

    StructuredPropertiesPatchBuilder builder =
        entity.getPatchBuilder("structuredProperties", StructuredPropertiesPatchBuilder.class);
    if (builder == null) {
      builder = new StructuredPropertiesPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("structuredProperties", builder);
    }

    builder.removeProperty(urn);
    return (T) this;
  }

  /**
   * Converts a property identifier (qualified name or full URN) to a proper Urn object.
   *
   * <p>This helper accepts flexible input formats:
   *
   * <ul>
   *   <li>Qualified property name: {@code "io.acryl.dataQuality.score"} → {@code
   *       "urn:li:structuredProperty:io.acryl.dataQuality.score"}
   *   <li>Full URN: {@code "urn:li:structuredProperty:io.acryl.dataQuality.score"} → unchanged
   * </ul>
   *
   * @param propertyUrn the property identifier (qualified name or full URN)
   * @return the Urn object
   * @throws IllegalArgumentException if the URN format is invalid
   */
  @Nonnull
  private static Urn makeStructuredPropertyUrn(@Nonnull String propertyUrn) {
    String fullUrn =
        propertyUrn.startsWith("urn:li:structuredProperty:")
            ? propertyUrn
            : "urn:li:structuredProperty:" + propertyUrn;
    try {
      return Urn.createFromString(fullUrn);
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(fullUrn, e);
    }
  }
}
