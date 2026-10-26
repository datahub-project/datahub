package com.linkedin.metadata.models.annotation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler.ResolutionResult;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

public class SearchableAnnotationHandlerTest {

  @Test
  public void testDegreePatchMergesOntoTheAnnotationAlreadyInEffect() {
    DataMap baseV3 = new DataMap();
    baseV3.put("searchLabel", "glossaryTerms");
    baseV3.put("queryByDefault", true);
    baseV3.put("includeSystemModifiedAt", false);
    DataMap base = new DataMap();
    base.put("fieldName", "editedFieldGlossaryTerms");
    base.put("fieldType", "URN");
    base.put("boostScore", 0.5);
    base.put("hasValuesFieldName", "hasGlossaryTerms");
    base.put("v3", baseV3);

    DataMap patchV3 = new DataMap();
    patchV3.put("degree", 1);
    patchV3.put("boostScore", 0.25);
    DataMap patch = new DataMap();
    patch.put("v3", patchV3);

    ResolutionResult result =
        new SearchableAnnotationHandler()
            .resolve(List.of(Pair.of("Searchable", patch), Pair.of("Searchable", base)), null);

    @SuppressWarnings("unchecked")
    Map<String, Object> merged =
        (Map<String, Object>) result.getResolvedResult().get(SearchableAnnotation.ANNOTATION_NAME);
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            merged, "urn", DataSchema.Type.STRING, "editableSchemaMetadata");

    assertEquals(annotation.getFieldName(), "editedFieldGlossaryTerms");
    assertEquals(annotation.getBoostScore(), 0.5);
    assertEquals(annotation.getHasValuesFieldName(), Optional.of("hasGlossaryTerms"));
    assertEquals(annotation.getV3IncludeSystemModifiedAt(), Optional.of(false));
    assertEquals(annotation.getDegree(), 1);
    assertEquals(annotation.getV3BoostScore(), Optional.of(0.25));
    assertTrue(annotation.isQueryByDefault());
    SearchableAnnotation resolved = annotation.resolvedForV3();
    assertTrue(resolved.isQueryByDefault());
    assertEquals(resolved.indexedFieldName().orElseThrow(), "glossaryTerms_1");
    assertEquals(resolved.getBoostScore(), 0.25);
    assertFalse(resolved.isIncludeSystemModifiedAt());
  }

  @Test
  public void testDegreePatchCanTurnOffQueryByDefault() {
    DataMap baseV3 = new DataMap();
    baseV3.put("searchLabel", "description");
    DataMap base = new DataMap();
    base.put("fieldName", "editedFieldDescriptions");
    base.put("fieldType", "TEXT");
    base.put("boostScore", 1.0);
    base.put("v3", baseV3);

    DataMap patchV3 = new DataMap();
    patchV3.put("degree", 1);
    patchV3.put("boostScore", 0.5);
    patchV3.put("queryByDefault", false);
    DataMap patch = new DataMap();
    patch.put("v3", patchV3);

    ResolutionResult result =
        new SearchableAnnotationHandler()
            .resolve(List.of(Pair.of("Searchable", patch), Pair.of("Searchable", base)), null);

    @SuppressWarnings("unchecked")
    Map<String, Object> merged =
        (Map<String, Object>) result.getResolvedResult().get(SearchableAnnotation.ANNOTATION_NAME);
    SearchableAnnotation annotation =
        SearchableAnnotation.fromPegasusAnnotationObject(
            merged, "description", DataSchema.Type.STRING, "editableSchemaMetadata");

    assertEquals(annotation.getFieldName(), "editedFieldDescriptions");
    assertTrue(annotation.isQueryByDefault());
    SearchableAnnotation resolved = annotation.resolvedForV3();
    assertEquals(resolved.indexedFieldName().orElseThrow(), "description_1");
    assertFalse(resolved.isQueryByDefault());
  }
}
