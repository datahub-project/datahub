package com.linkedin.metadata.config.usage.cigate.restli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.cigate.HandlerExemptionSnapshot;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryRepoPaths;
import java.nio.file.Files;
import java.nio.file.Path;

public final class RestliUsageExemptionSnapshotGenerator {

  private RestliUsageExemptionSnapshotGenerator() {}

  public static void main(String[] args) throws Exception {
    Path repoRoot = UsageRegistryRepoPaths.repoRoot();
    Path output =
        args.length > 0
            ? Path.of(args[0])
            : repoRoot.resolve(
                "metadata-service/configuration/src/test/resources/restli_usage_exemptions.snapshot.yaml");
    var scanned = RestliInstrumentationScanner.scan(repoRoot);
    ObjectMapper mapper = UsageYamlMapper.create();
    HandlerExemptionSnapshot existing =
        Files.isRegularFile(output)
            ? HandlerExemptionSnapshot.fromJsonPath(output, mapper)
            : HandlerExemptionSnapshot.empty();
    HandlerExemptionSnapshot reconciled = HandlerExemptionSnapshot.reconcile(scanned, existing);
    Files.createDirectories(output.getParent());
    Files.writeString(output, reconciled.toJson(mapper));
    System.out.println("Wrote Rest.li usage exemption snapshot: " + output);
  }
}
