package com.linkedin.metadata.config.usage.cigate.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryRepoPaths;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import java.nio.file.Files;
import java.nio.file.Path;

public final class GraphqlUsageExemptionSnapshotGenerator {

  private GraphqlUsageExemptionSnapshotGenerator() {}

  public static void main(String[] args) throws Exception {
    Path repoRoot = UsageRegistryRepoPaths.repoRoot();
    Path output =
        args.length > 0
            ? Path.of(args[0])
            : repoRoot.resolve(
                "metadata-service/configuration/src/test/resources/graphql_usage_exemptions.snapshot.yaml");
    GraphqlUsageSurface surface = GraphqlUsageSurfaceExtractor.extract(repoRoot);
    GraphqlUsageCoverageClassifier classifier =
        GraphqlUsageCoverageClassifier.fromBundledYaml(
            new UsageOperationsLoader(UsageYamlMapper.create()));
    ObjectMapper mapper = UsageYamlMapper.create();
    GraphqlExemptionSnapshot existing =
        Files.isRegularFile(output)
            ? GraphqlExemptionSnapshot.fromJsonPath(output, mapper)
            : GraphqlExemptionSnapshot.empty();
    GraphqlExemptionSnapshot reconciled =
        GraphqlExemptionSnapshot.reconcile(surface, existing, classifier);
    Files.createDirectories(output.getParent());
    Files.writeString(output, reconciled.toJson(mapper));
    System.out.println("Wrote GraphQL usage exemption snapshot: " + output);
  }
}
