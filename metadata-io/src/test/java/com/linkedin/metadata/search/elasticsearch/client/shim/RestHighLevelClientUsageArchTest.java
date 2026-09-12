package com.linkedin.metadata.search.elasticsearch.client.shim;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import org.testng.annotations.Test;

/**
 * The deprecated {@code RestHighLevelClient} is retired as the OpenSearch transport: OS2/OS3 I/O
 * goes through {@code OpenSearchSearchClientShim} (low-level RestClient + the RHLC jar's request
 * converters/parsers via {@code OpenSearchShimBridge}). The RHLC class itself may only be used by
 * the ES 7.x compatibility path and the bridge. This rule keeps new RHLC I/O from creeping back in.
 */
public class RestHighLevelClientUsageArchTest {

  @Test
  public void restHighLevelClientOnlyUsedByEs7PathAndBridge() {
    // DataHub classes only — the RHLC jar's own internals in org.opensearch.client obviously
    // reference RestHighLevelClient, and the bridge (also in that package) is allowed to.
    JavaClasses productionClasses =
        new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages("com.linkedin.metadata");

    ArchRuleDefinition.noClasses()
        .that()
        .doNotHaveFullyQualifiedName(
            "com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearch2SearchClientShim")
        .and()
        .doNotHaveFullyQualifiedName(
            "com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es7CompatibilitySearchClientShim")
        .should()
        .dependOnClassesThat()
        .haveFullyQualifiedName("org.opensearch.client.RestHighLevelClient")
        .because(
            "RestHighLevelClient is deprecated and retired as the OpenSearch transport; use the"
                + " unified OpenSearchSearchClientShim (low-level RestClient + OpenSearchShimBridge"
                + " converters). Only the ES 7.x compatibility shim may still perform RHLC I/O.")
        .check(productionClasses);
  }
}
