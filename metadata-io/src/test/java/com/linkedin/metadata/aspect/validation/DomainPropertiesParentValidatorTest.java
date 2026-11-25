package com.linkedin.metadata.aspect.validation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import java.util.List;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainPropertiesParentValidatorTest {

  private static final Urn DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:domain");
  private static final Urn PARENT_DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:parent");

  private DomainPropertiesParentValidator validator;

  @BeforeMethod
  public void setUp() {
    validator =
        new DomainPropertiesParentValidator()
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
        validate(domainProperties(DOMAIN_URN, "domain"), null).toList();

    assertEquals(exceptions.size(), 1);
  }

  @Test
  public void allowsAbsentOrDistinctParent() {
    assertTrue(validate(new DomainProperties().setName("domain"), null).findAny().isEmpty());
    assertTrue(validate(domainProperties(PARENT_DOMAIN_URN, "domain"), null).findAny().isEmpty());
  }

  @Test
  public void allowsUpdateThatPreservesExistingSelfParent() {
    DomainProperties stored = domainProperties(DOMAIN_URN, "domain");
    DomainProperties renamed = domainProperties(DOMAIN_URN, "renamed");

    assertTrue(validate(renamed, stored).findAny().isEmpty());
  }

  @Test
  public void rejectsReplacingDistinctParentWithSelf() {
    DomainProperties stored = domainProperties(PARENT_DOMAIN_URN, "domain");
    DomainProperties proposed = domainProperties(DOMAIN_URN, "domain");

    assertEquals(validate(proposed, stored).count(), 1);
  }

  @Test
  public void allowsClearingOrReplacingSelfParent() {
    DomainProperties stored = domainProperties(DOMAIN_URN, "domain");

    assertTrue(validate(new DomainProperties().setName("renamed"), stored).findAny().isEmpty());
    assertTrue(validate(domainProperties(PARENT_DOMAIN_URN, "domain"), stored).findAny().isEmpty());
  }

  private java.util.stream.Stream<AspectValidationException> validate(
      DomainProperties proposed, @Nullable DomainProperties previous) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(DOMAIN_URN);
    when(item.getAspect(DomainProperties.class)).thenReturn(proposed);
    if (previous != null) {
      when(item.getPreviousRecordTemplate()).thenReturn(previous);
      when(item.getPreviousAspect(DomainProperties.class)).thenReturn(previous);
    }
    return validator.validatePreCommitAspects(
        OperationFingerprint.EMPTY, List.of(item), mock(RetrieverContext.class));
  }

  private static DomainProperties domainProperties(Urn parentDomain, String name) {
    return new DomainProperties().setName(name).setParentDomain(parentDomain);
  }
}
