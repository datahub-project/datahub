package com.linkedin.metadata.aspect.plugins.hooks;

import static org.testng.Assert.assertEquals;

import com.datahub.test.TestEntityProfile;
import com.linkedin.common.AuditStamp;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class MutationPluginTest {
  public static String REGISTRY_FILE = "test-entity-registry-plugins-1.yml";

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testCustomMutator() {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class.getClassLoader().getResourceAsStream(REGISTRY_FILE));

    List<MutationHook> mutators =
        configEntityRegistry.getMutationHooks(ChangeType.UPSERT, "*", "schemaMetadata");
    assertEquals(
        mutators,
        List.of(
            new TestMutator(
                AspectPluginConfig.builder()
                    .className(
                        "com.linkedin.metadata.aspect.plugins.hooks.MutationPluginTest$TestMutator")
                    .supportedOperations(List.of("UPSERT"))
                    .enabled(true)
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName("*")
                                .aspectName("schemaMetadata")
                                .build()))
                    .build())));
  }

  public static class TestMutator extends MutationHook {

    public TestMutator(AspectPluginConfig aspectPluginConfig) {
      super(aspectPluginConfig);
    }

    @Override
    protected void mutate(
        @Nonnull ChangeType changeType,
        @Nonnull EntitySpec entitySpec,
        @Nonnull AspectSpec aspectSpec,
        @Nullable RecordTemplate oldAspectValue,
        @Nullable RecordTemplate newAspectValue,
        @Nullable SystemMetadata oldSystemMetadata,
        @Nullable SystemMetadata newSystemMetadata,
        @Nonnull AuditStamp auditStamp,
        @Nonnull AspectRetriever aspectRetriever) {}
  }
}
