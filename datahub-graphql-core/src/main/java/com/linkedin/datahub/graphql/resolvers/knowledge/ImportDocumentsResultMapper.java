package com.linkedin.datahub.graphql.resolvers.knowledge;

import com.linkedin.datahub.graphql.generated.ImportDocumentsResult;
import com.linkedin.metadata.service.docimport.ImportResult;
import java.util.ArrayList;
import javax.annotation.Nonnull;

/** Maps service-layer document import results to GraphQL types. */
public final class ImportDocumentsResultMapper {

  private ImportDocumentsResultMapper() {}

  @Nonnull
  public static ImportDocumentsResult toGraphQL(@Nonnull ImportResult result) {
    ImportDocumentsResult graphqlResult = new ImportDocumentsResult();
    graphqlResult.setCreatedCount(result.getCreatedCount());
    graphqlResult.setUpdatedCount(result.getUpdatedCount());
    graphqlResult.setFailedCount(result.getFailedCount());
    graphqlResult.setErrors(new ArrayList<>(result.getErrors()));
    graphqlResult.setDocumentUrns(new ArrayList<>(result.getDocumentUrns()));
    return graphqlResult;
  }
}
