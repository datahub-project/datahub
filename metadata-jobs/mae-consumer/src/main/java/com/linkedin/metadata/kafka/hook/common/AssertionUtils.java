package com.linkedin.metadata.kafka.hook.common;

import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.urn.Urn;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

public class AssertionUtils {
  @Nonnull
  public static List<Urn> extractAssertionEntities(@Nonnull final AssertionInfo assertionInfo) {
    if (assertionInfo.hasDatasetAssertion()) {
      DatasetAssertionInfo datasetAssertion = assertionInfo.getDatasetAssertion();
      if (datasetAssertion.hasDataset()) {
        return ImmutableList.of(datasetAssertion.getDataset());
      }
    }
    if (assertionInfo.hasFreshnessAssertion()) {
      FreshnessAssertionInfo freshnessAssertion = assertionInfo.getFreshnessAssertion();
      if (freshnessAssertion.hasEntity()) {
        return ImmutableList.of(freshnessAssertion.getEntity());
      }
    }
    if (assertionInfo.hasVolumeAssertion()) {
      VolumeAssertionInfo volumeAssertion = assertionInfo.getVolumeAssertion();
      if (volumeAssertion.hasEntity()) {
        return ImmutableList.of(volumeAssertion.getEntity());
      }
    }
    if (assertionInfo.hasSqlAssertion()) {
      SqlAssertionInfo sqlAssertion = assertionInfo.getSqlAssertion();
      if (sqlAssertion.hasEntity()) {
        return ImmutableList.of(sqlAssertion.getEntity());
      }
    }
    if (assertionInfo.hasFieldAssertion()) {
      FieldAssertionInfo fieldAssertion = assertionInfo.getFieldAssertion();
      if (fieldAssertion.hasEntity()) {
        return ImmutableList.of(fieldAssertion.getEntity());
      }
    }
    if (assertionInfo.hasSchemaAssertion()) {
      SchemaAssertionInfo schemaAssertion = assertionInfo.getSchemaAssertion();
      if (schemaAssertion.hasEntity()) {
        return ImmutableList.of(schemaAssertion.getEntity());
      }
    }
    if (assertionInfo.hasCustomAssertion()) {
      CustomAssertionInfo customAssertion = assertionInfo.getCustomAssertion();
      if (customAssertion.hasEntity()) {
        return ImmutableList.of(customAssertion.getEntity());
      }
    }
    return Collections.emptyList();
  }
}
