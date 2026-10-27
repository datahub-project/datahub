package com.linkedin.datahub.graphql.resolvers.knowledge;

import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.generated.ImportDocumentsResult;
import com.linkedin.metadata.service.docimport.ImportResult;
import org.testng.annotations.Test;

public class ImportDocumentsResultMapperTest {

  @Test
  public void testToGraphQLMapsCountsAndCollections() {
    ImportResult serviceResult = new ImportResult();
    serviceResult.recordSuccess("urn:li:document:created", false);
    serviceResult.recordSuccess("urn:li:document:updated", true);
    serviceResult.recordFailure("upload.bad", "failed to ingest");

    ImportDocumentsResult graphqlResult = ImportDocumentsResultMapper.toGraphQL(serviceResult);

    assertEquals(graphqlResult.getCreatedCount(), Integer.valueOf(1));
    assertEquals(graphqlResult.getUpdatedCount(), Integer.valueOf(1));
    assertEquals(graphqlResult.getFailedCount(), Integer.valueOf(1));
    assertEquals(graphqlResult.getDocumentUrns().size(), 2);
    assertTrue(graphqlResult.getDocumentUrns().contains("urn:li:document:created"));
    assertTrue(graphqlResult.getDocumentUrns().contains("urn:li:document:updated"));
    assertEquals(graphqlResult.getErrors().size(), 1);
    assertEquals(graphqlResult.getErrors().get(0), "upload.bad: failed to ingest");
  }

  @Test
  public void testToGraphQLEmptyResult() {
    ImportDocumentsResult graphqlResult = ImportDocumentsResultMapper.toGraphQL(new ImportResult());

    assertEquals(graphqlResult.getCreatedCount(), Integer.valueOf(0));
    assertEquals(graphqlResult.getUpdatedCount(), Integer.valueOf(0));
    assertEquals(graphqlResult.getFailedCount(), Integer.valueOf(0));
    assertTrue(graphqlResult.getDocumentUrns().isEmpty());
    assertTrue(graphqlResult.getErrors().isEmpty());
  }
}
