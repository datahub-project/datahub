package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.key.CustomFabricTypeKey;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;

public class CustomFabricTypeKeyValidatorTest {

  private final AtomicBoolean enabled = new AtomicBoolean(true);
  private final CustomFabricTypeKeyValidator validator =
      new CustomFabricTypeKeyValidator()
          .setCustomTypesEnabled(enabled::get)
          .setConfig(
              AspectPluginConfig.builder()
                  .className("test")
                  .enabled(true)
                  .supportedOperations(List.of("CREATE"))
                  .supportedEntityAspectNames(List.of())
                  .build());

  @Test
  public void acceptsAWellFormedNewId() {
    enabled.set(true);
    assertEquals(errors("MY_ENV", false), 0);
  }

  @Test
  public void rejectsBuiltInSymbolsInAnyCase() {
    enabled.set(true);
    assertEquals(errors("PROD", false), 1);
    assertEquals(errors("prod", false), 1);
    assertEquals(errors("Prod", false), 1);
  }

  @Test
  public void rejectsMalformedIds() {
    enabled.set(true);
    assertEquals(errors("my_env", false), 1);
    assertEquals(errors("MY-ENV", false), 1);
    assertEquals(errors("1ENV", false), 1);
  }

  @Test
  public void rejectsCreationWhenCustomTypesAreOff() {
    enabled.set(false);
    assertEquals(errors("MY_ENV", false), 1);
  }

  @Test
  public void existingTypesAreNotRechecked() {
    enabled.set(false);
    assertEquals(errors("MY_ENV", true), 0);
  }

  private int errors(String id, boolean exists) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(UrnUtils.getUrn("urn:li:customFabricType:" + id));
    when(item.getAspectName()).thenReturn(CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME);
    when(item.getChangeType()).thenReturn(ChangeType.CREATE);
    if (exists) {
      when(item.getPreviousRecordTemplate()).thenReturn(new CustomFabricTypeKey().setId(id));
    }
    return (int)
        validator
            .validatePreCommitAspects(
                OperationFingerprint.EMPTY, List.of(item), mock(RetrieverContext.class))
            .count();
  }
}
