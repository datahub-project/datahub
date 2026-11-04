package com.linkedin.datahub.upgrade.loadindices;

import lombok.Data;

@Data
public class LoadIndicesResult {
  public int rowsProcessed = 0;
  // Distinct URNs seen in URN order; each yields one V3 document, so this is the fill's expected
  // document count.
  public long urnsProcessed = 0;
  public long timeSqlQueryMs = 0;
  public long timeElasticsearchWriteMs = 0;
  // Aspects skipped by the skipConversionErrors escape hatch (0 unless the hatch is enabled).
  public long conversionErrorsSkipped = 0;

  @Override
  public String toString() {
    return String.format(
        "LoadIndicesResult{rowsProcessed=%d, urnsProcessed=%d, timeSqlQueryMs=%d, timeElasticsearchWriteMs=%d, conversionErrorsSkipped=%d}",
        rowsProcessed,
        urnsProcessed,
        timeSqlQueryMs,
        timeElasticsearchWriteMs,
        conversionErrorsSkipped);
  }
}
