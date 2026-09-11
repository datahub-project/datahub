package com.linkedin.metadata.config.resolver;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.ConfigEnrichment;
import com.datahub.context.EnrichmentBundle;
import com.datahub.context.OperationFingerprint;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

public class ConfigEnrichmentTest {

  /** Answers one key; every other key falls through to the caller's default. */
  private record StubConfigEnrichment(String answeredKey, Object answer)
      implements ConfigEnrichment {

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T> T resolve(
        @Nonnull OperationFingerprint operation, @Nonnull String key, @Nonnull T defaultValue) {
      return answeredKey.equals(key) ? (T) answer : defaultValue;
    }
  }

  @Test
  public void readsServeTheDefaultWhenNoEnrichmentIsStamped() {
    assertTrue(OperationFingerprint.EMPTY.getConfig(ConfigKeyConstants.Views.ENABLED, true));
    assertTrue(
        TestOperationContexts.systemContextNoSearchAuthorization()
            .getConfig(ConfigKeyConstants.Views.ENABLED, true));
  }

  @Test
  public void readsResolveThroughTheStampedEnrichment() {
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    OperationContext opContext =
        base.toBuilder()
            .enrichmentBundle(
                base.getEnrichmentBundle()
                    .plus(new StubConfigEnrichment(ConfigKeyConstants.Views.ENABLED, false)))
            .build(base.getSessionActorContext(), false);

    assertFalse(opContext.getConfig(ConfigKeyConstants.Views.ENABLED, true));
    assertEquals(
        opContext.getConfig(ConfigKeyConstants.SearchBar.API_VARIANT, "DEFAULT"), "DEFAULT");
  }

  @Test
  public void implementationsAreStoredUnderTheConfigEnrichmentKey() {
    EnrichmentBundle bundle =
        EnrichmentBundle.of(new StubConfigEnrichment(ConfigKeyConstants.Views.ENABLED, false));

    assertTrue(bundle.get(ConfigEnrichment.class).isPresent());
  }

  /** The generated constants carry the exact authored yaml spelling. */
  @Test
  public void generatedConstantsCarryExactYamlKeySpelling() {
    assertEquals(ConfigKeyConstants.Views.ENABLED, "views.enabled");
    assertEquals(ConfigKeyConstants.SearchBar.API_VARIANT, "searchBar.apiVariant");
  }
}
