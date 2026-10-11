package com.linkedin.metadata.service.docimport;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Data;

/** Result of a document import operation. */
@Data
public class ImportResult {
  private int createdCount;
  private int updatedCount;
  private int failedCount;
  @Nonnull private final List<String> errors = new ArrayList<>();
  @Nonnull private final List<String> documentUrns = new ArrayList<>();

  public void recordSuccess(@Nonnull String urn, boolean isUpdate) {
    if (isUpdate) {
      updatedCount++;
    } else {
      createdCount++;
    }
    documentUrns.add(urn);
  }

  public void recordFailure(@Nonnull String sourceId, @Nonnull String error) {
    failedCount++;
    errors.add(sourceId + ": " + error);
  }

  public int getTotal() {
    return createdCount + updatedCount + failedCount;
  }
}
