package com.linkedin.metadata.aspect.plugins.hooks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.testng.annotations.Test;

public class MCPObserverResilienceTest {

  @Test
  public void throwingObserverDoesNotFailDispatch() {
    ThrowingMCPObserver throwing = new ThrowingMCPObserver();
    throwing.setConfig(
        AspectPluginConfig.builder()
            .className(ThrowingMCPObserver.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName("*")
                        .build()))
            .build());

    EntityRegistry registry = mock(EntityRegistry.class);
    when(registry.getAllMCPObservers()).thenReturn(List.of(throwing));

    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getEntityRegistry()).thenReturn(registry);

    RetrieverContext rc = mock(RetrieverContext.class);
    when(rc.getAspectRetriever()).thenReturn(retriever);

    try {
      AspectsBatch.applyMCPObservers(Collections.<BatchItem>emptyList(), rc);
    } catch (Throwable t) {
      fail("applyMCPObservers must not propagate observer errors", t);
    }
  }

  @Getter
  @Setter
  @Accessors(chain = true)
  public static class ThrowingMCPObserver extends MCPObserver {

    private AspectPluginConfig config;

    @Override
    protected void observeMCPs(
        Collection<? extends BatchItem> items, @Nonnull RetrieverContext retrieverContext) {
      throw new ClassCastException("null");
    }
  }
}
