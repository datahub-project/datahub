package com.linkedin.datahub.graphql.types.ingest.secret.mapper;

import static com.linkedin.metadata.Constants.SECRET_VALUE_ASPECT_NAME;

import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.secret.DataHubSecretValue;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class DataHubSecretValueMapper {

  public static final DataHubSecretValueMapper INSTANCE = new DataHubSecretValueMapper();

  public static DataHubSecretValue map(
      EntityResponse fromSecret,
      @Nonnull final String name,
      @Nonnull final String value,
      String description,
      AuditStamp auditStamp) {
    return INSTANCE.apply(fromSecret, name, value, description, auditStamp);
  }

  public DataHubSecretValue apply(
      EntityResponse existingSecret,
      @Nonnull final String name,
      @Nonnull final String value,
      String description,
      AuditStamp auditStamp) {
    final DataHubSecretValue result;
    if (Objects.nonNull(existingSecret)) {
      result =
          new DataHubSecretValue(
              existingSecret.getAspects().get(SECRET_VALUE_ASPECT_NAME).getValue().data());
    } else {
      result = new DataHubSecretValue();
    }

    result.setName(name);
    result.setValue(value);
    result.setDescription(description, SetMode.IGNORE_NULL);
    if (Objects.nonNull(auditStamp)) {
      result.setCreated(auditStamp);
    }

    return result;
  }
}
