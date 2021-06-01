package com.linkedin.metadata;

import com.datahub.test.TestEntitySnapshot;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;


public class TestEntitySpecBuilder {
  private static final EntitySpecBuilder ENTITY_SPEC_BUILDER = new EntitySpecBuilder();

  private TestEntitySpecBuilder() {
  }

  public static EntitySpec getSpec() {
    return ENTITY_SPEC_BUILDER.buildEntitySpec(new TestEntitySnapshot().schema());
  }
}
