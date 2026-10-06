package com.linkedin.metadata.aliases.sideeffects;

import static com.linkedin.metadata.Constants.ALIASES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Aliases;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.utils.AliasesUtils;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Derives the system-computed {@code aliases.lowercasedUrn} field from the entity's URN. Keyed on
 * the dataset key aspect so it runs once, at creation.
 *
 * <p>Skipped when the URN already equals its lowercased form, so a lookup has to match {@code urn}
 * OR {@code lowercasedUrn}.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class AliasesSideEffect extends MCPSideEffect {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return changeMCPs.stream().flatMap(item -> buildLowercasedUrn(item, retrieverContext));
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<MCLItem> mclItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  private static Stream<ChangeMCP> buildLowercasedUrn(
      ChangeMCP changeMCP, @Nonnull RetrieverContext retrieverContext) {
    Urn urn = changeMCP.getUrn();
    if (!DATASET_ENTITY_NAME.equals(urn.getEntityType())) {
      return Stream.empty();
    }

    final DatasetUrn lowercasedUrn;
    try {
      lowercasedUrn = AliasesUtils.lowercaseDatasetUrn(urn);
    } catch (URISyntaxException e) {
      log.warn("Unable to compute lowercasedUrn for {}", urn, e);
      return Stream.empty();
    }

    if (lowercasedUrn.equals(urn)) {
      return Stream.empty();
    }

    Aliases aspect = new Aliases().setLowercasedUrn(lowercasedUrn);
    return Stream.of(
        ChangeItemImpl.builder()
            .urn(urn)
            .aspectName(ALIASES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .recordTemplate(aspect)
            .auditStamp(changeMCP.getAuditStamp())
            .systemMetadata(changeMCP.getSystemMetadata())
            .build(retrieverContext.getAspectRetriever()));
  }
}
