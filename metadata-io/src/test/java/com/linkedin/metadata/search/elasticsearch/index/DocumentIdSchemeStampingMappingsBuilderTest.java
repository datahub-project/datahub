package com.linkedin.metadata.search.elasticsearch.index;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

public class DocumentIdSchemeStampingMappingsBuilderTest {

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private final V3DocumentIdScheme scheme =
      new V3DocumentIdScheme(new Sha256UrnEntityDocumentIdHasher());

  @Test
  public void testEveryEmittedIndexMappingCarriesTheRunningScheme() {
    MappingsBuilder delegate = mock(MappingsBuilder.class);
    Map<String, Object> properties = Map.of("urn", Map.of("type", "keyword"));
    when(delegate.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(
            List.of(
                MappingsBuilder.IndexMapping.builder()
                    .indexName("primaryindex_v3")
                    .mappings(Map.of("properties", properties))
                    .build(),
                MappingsBuilder.IndexMapping.builder()
                    .indexName("secondaryindex_v3")
                    .mappings(Map.of("properties", properties))
                    .build()));

    List<MappingsBuilder.IndexMapping> stamped =
        List.copyOf(
            new DocumentIdSchemeStampingMappingsBuilder(delegate, scheme)
                .getIndexMappings(opContext));

    assertEquals(stamped.size(), 2);
    for (MappingsBuilder.IndexMapping indexMapping : stamped) {
      assertEquals(
          V3DocumentIdScheme.stampedOn(indexMapping.getMappings()),
          Optional.of(Sha256UrnEntityDocumentIdHasher.SCHEME));
      assertEquals(indexMapping.getMappings().get("properties"), properties);
      assertFalse(scheme.requiresDocumentRebuild(indexMapping.getMappings()));
    }
  }
}
