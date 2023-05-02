package mock;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.TimeseriesFieldCollectionSpec;
import com.linkedin.metadata.models.TimeseriesFieldSpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import java.util.List;
import javax.annotation.Nonnull;


public class MockAspectSpec extends AspectSpec {
  public MockAspectSpec(@Nonnull AspectAnnotation aspectAnnotation,
      @Nonnull List<SearchableFieldSpec> searchableFieldSpecs,
      @Nonnull List<SearchScoreFieldSpec> searchScoreFieldSpecs,
      @Nonnull List<RelationshipFieldSpec> relationshipFieldSpecs,
      @Nonnull List<TimeseriesFieldSpec> timeseriesFieldSpecs,
      @Nonnull List<TimeseriesFieldCollectionSpec> timeseriesFieldCollectionSpecs, RecordDataSchema schema,
      Class<RecordTemplate> aspectClass) {
    super(aspectAnnotation, searchableFieldSpecs, searchScoreFieldSpecs, relationshipFieldSpecs, timeseriesFieldSpecs,
        timeseriesFieldCollectionSpecs, schema, aspectClass);
  }
}
