package com.linkedin.metadata.search.elasticsearch.client.shim;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import org.testng.annotations.Test;

/**
 * The deprecated {@code RestHighLevelClient} is retired as a DataHub I/O transport: OS2/OS3 goes
 * through {@code OpenSearchSearchClientShim} (low-level RestClient + the RHLC jar's request
 * converters/parsers via {@code OpenSearchShimBridge}). This rule keeps new RHLC I/O from creeping
 * back into {@code com.linkedin.metadata}.
 */
public class RestHighLevelClientUsageArchTest {

  @Test
  public void restHighLevelClientNotUsedForIoInDatahubMetadata() {
    JavaClasses productionClasses =
        new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages("com.linkedin.metadata");

    ArchRuleDefinition.noClasses()
        .should()
        .dependOnClassesThat()
        .haveFullyQualifiedName("org.opensearch.client.RestHighLevelClient")
        .because(
            "RestHighLevelClient is deprecated and retired as the OpenSearch transport; use the"
                + " unified OpenSearchSearchClientShim (low-level RestClient + OpenSearchShimBridge"
                + " converters).")
        .check(productionClasses);
  }
}
