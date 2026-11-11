package com.datahub.authorization;

import lombok.Data;

/** Authorization settings for schema-only entity and lineage registry reads. */
@Data
public class RegistryApiConfiguration {
  private boolean authenticatedReadEnabled = true;
}
