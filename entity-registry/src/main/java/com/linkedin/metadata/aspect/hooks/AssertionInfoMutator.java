package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class AssertionInfoMutator extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  /*
   * Write's the assertion's entity URN to the assertionInfo aspect. This allows
   * us to search assertions by the assertion's entity URN.
   */
  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {

    List<Pair<ChangeMCP, Boolean>> results = new LinkedList<>();

    for (ChangeMCP item : changeMCPS) {
      if (changeTypeFilter(item) && aspectFilter(item)) {
        results.add(Pair.of(item, processAssertionInfoAspect(item)));
      } else {
        // no op
        results.add(Pair.of(item, false));
      }
    }

    return results.stream();
  }

  private static boolean changeTypeFilter(BatchItem item) {
    return !ChangeType.DELETE.equals(item.getChangeType());
  }

  private static boolean aspectFilter(ReadItem item) {
    return ASSERTION_INFO_ASPECT_NAME.equals(item.getAspectName());
  }

  private static boolean processAssertionInfoAspect(ChangeMCP item) {
    boolean mutated = false;
    final AssertionInfo next = item.getAspect(AssertionInfo.class);
    if (next != null && !next.hasEntityUrn()) {
      final Urn entityUrn = getEntityFromAssertionInfo(next);
      if (entityUrn != null) {
        next.setEntityUrn(entityUrn);
        mutated = true;
      }
    }
    return mutated;
  }

  public static Urn getEntityFromAssertionInfo(AssertionInfo assertionInfo) {
    if (assertionInfo == null) {
      return null;
    }
    if (!assertionInfo.hasType()) {
      log.warn(
          "AssertionInfo missing type; cannot derive entityUrn. assertionInfo={}", assertionInfo);
      return null;
    }

    switch (assertionInfo.getType()) {
      case DATASET:
        final DatasetAssertionInfo datasetAssertionInfo = assertionInfo.getDatasetAssertion();
        if (datasetAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=DATASET but datasetAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return datasetAssertionInfo.getDataset();
      case FRESHNESS:
        final FreshnessAssertionInfo freshnessAssertionInfo = assertionInfo.getFreshnessAssertion();
        if (freshnessAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=FRESHNESS but freshnessAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return freshnessAssertionInfo.getEntity();
      case VOLUME:
        final VolumeAssertionInfo volumeAssertionInfo = assertionInfo.getVolumeAssertion();
        if (volumeAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=VOLUME but volumeAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return volumeAssertionInfo.getEntity();
      case SQL:
        final SqlAssertionInfo sqlAssertionInfo = assertionInfo.getSqlAssertion();
        if (sqlAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=SQL but sqlAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return sqlAssertionInfo.getEntity();
      case FIELD:
        final FieldAssertionInfo fieldAssertionInfo = assertionInfo.getFieldAssertion();
        if (fieldAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=FIELD but fieldAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return fieldAssertionInfo.getEntity();
      case DATA_SCHEMA:
        final SchemaAssertionInfo schemaAssertionInfo = assertionInfo.getSchemaAssertion();
        if (schemaAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=DATA_SCHEMA but schemaAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return schemaAssertionInfo.getEntity();
      case CUSTOM:
        final CustomAssertionInfo customAssertionInfo = assertionInfo.getCustomAssertion();
        if (customAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=CUSTOM but customAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return customAssertionInfo.getEntity();
      default:
        log.warn(
            "Unsupported AssertionInfo type {}; cannot derive entityUrn. assertionInfo={}",
            assertionInfo.getType(),
            assertionInfo);
        return null;
    }
  }
}
