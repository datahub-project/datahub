/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package mock;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.SearchableRefFieldSpec;
import com.linkedin.metadata.models.TimeseriesFieldCollectionSpec;
import com.linkedin.metadata.models.TimeseriesFieldSpec;
import com.linkedin.metadata.models.UrnValidationFieldSpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import java.util.List;
import javax.annotation.Nonnull;

public class MockAspectSpec extends AspectSpec {
  public MockAspectSpec(
      @Nonnull AspectAnnotation aspectAnnotation,
      @Nonnull List<SearchableFieldSpec> searchableFieldSpecs,
      @Nonnull List<SearchScoreFieldSpec> searchScoreFieldSpecs,
      @Nonnull List<RelationshipFieldSpec> relationshipFieldSpecs,
      @Nonnull List<TimeseriesFieldSpec> timeseriesFieldSpecs,
      @Nonnull List<TimeseriesFieldCollectionSpec> timeseriesFieldCollectionSpecs,
      @Nonnull final List<SearchableRefFieldSpec> searchableRefFieldSpecs,
      @Nonnull final List<UrnValidationFieldSpec> urnValidationFieldSpecs,
      RecordDataSchema schema,
      Class<RecordTemplate> aspectClass) {
    super(
        aspectAnnotation,
        searchableFieldSpecs,
        searchScoreFieldSpecs,
        relationshipFieldSpecs,
        timeseriesFieldSpecs,
        timeseriesFieldCollectionSpecs,
        searchableRefFieldSpecs,
        urnValidationFieldSpecs,
        schema,
        aspectClass);
  }
}
