package io.datahubproject.models.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatabaseKey extends ContainerKey {
  private String database;

  private static final String DATABASE_MAP_FIELD = "database";

  @Override
  public Map<String, String> guidDict() {
    // Get the parent's GUID dictionary first
    Map<String, String> bag = super.guidDict();

    // Add the database field if it's not null
    if (database != null) {
      bag.put(DATABASE_MAP_FIELD, database);
    }

    return bag;
  }
}
