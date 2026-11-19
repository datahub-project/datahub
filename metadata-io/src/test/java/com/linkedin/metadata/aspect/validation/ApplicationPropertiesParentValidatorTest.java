package com.linkedin.metadata.aspect.validation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.application.ApplicationProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import java.util.List;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ApplicationPropertiesParentValidatorTest {

  private static final Urn APP_URN = UrnUtils.getUrn("urn:li:application:app");
  private static final Urn PARENT_APP_URN = UrnUtils.getUrn("urn:li:application:parent");

  private ApplicationPropertiesParentValidator validator;

  @BeforeMethod
  public void setUp() {
    validator =
        new ApplicationPropertiesParentValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className("test")
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(List.of())
                    .build());
  }

  @Test
  public void rejectsNewSelfParent() {
    List<AspectValidationException> exceptions =
        validate(applicationProperties(APP_URN, "app"), null).toList();

    assertEquals(exceptions.size(), 1);
  }

  @Test
  public void allowsAbsentOrDistinctParent() {
    assertTrue(validate(new ApplicationProperties().setName("app"), null).findAny().isEmpty());
    assertTrue(validate(applicationProperties(PARENT_APP_URN, "app"), null).findAny().isEmpty());
  }

  @Test
  public void allowsUpdateThatPreservesExistingSelfParent() {
    ApplicationProperties stored = applicationProperties(APP_URN, "app");
    ApplicationProperties renamed = applicationProperties(APP_URN, "renamed");

    assertTrue(validate(renamed, stored).findAny().isEmpty());
  }

  @Test
  public void rejectsReplacingDistinctParentWithSelf() {
    ApplicationProperties stored = applicationProperties(PARENT_APP_URN, "app");
    ApplicationProperties proposed = applicationProperties(APP_URN, "app");

    assertEquals(validate(proposed, stored).count(), 1);
  }

  @Test
  public void allowsClearingOrReplacingSelfParent() {
    ApplicationProperties stored = applicationProperties(APP_URN, "app");

    assertTrue(
        validate(new ApplicationProperties().setName("renamed"), stored).findAny().isEmpty());
    assertTrue(validate(applicationProperties(PARENT_APP_URN, "app"), stored).findAny().isEmpty());
  }

  private java.util.stream.Stream<AspectValidationException> validate(
      ApplicationProperties proposed, @Nullable ApplicationProperties previous) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(APP_URN);
    when(item.getAspect(ApplicationProperties.class)).thenReturn(proposed);
    if (previous != null) {
      when(item.getPreviousRecordTemplate()).thenReturn(previous);
      when(item.getPreviousAspect(ApplicationProperties.class)).thenReturn(previous);
    }
    return validator.validatePreCommitAspects(
        OperationFingerprint.EMPTY, List.of(item), mock(RetrieverContext.class));
  }

  private static ApplicationProperties applicationProperties(Urn parentApplication, String name) {
    return new ApplicationProperties().setName(name).setParentApplication(parentApplication);
  }
}
