package com.linkedin.metadata.search.embedding;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

/**
 * Benchmark harness for OnnxEmbeddingProvider. Not a unit test — gated behind the ONNX_MODEL_DIR
 * system property so it doesn't run in CI.
 *
 * <p>Run with: {@code ./gradlew :metadata-io:test --tests "*OnnxBenchmark*"
 * -DONNX_MODEL_DIR=/path/to/model}
 */
public class OnnxBenchmarkTest {

  private static final List<String> SHORT_QUERIES =
      List.of(
          "Monthly Revenue Dashboard",
          "customer_orders",
          "User Engagement Metrics",
          "prod.warehouse.fact_sales",
          "ETL Pipeline Status",
          "data_quality_score",
          "Marketing Campaign ROI",
          "employee_directory");

  private static final List<String> LONG_QUERIES =
      List.of(
          "This dataset contains all customer transaction records from the e-commerce platform, "
              + "including order IDs, product SKUs, quantities, prices, discounts applied, "
              + "shipping addresses, and payment method details. Updated hourly from the "
              + "production MySQL database via Debezium CDC.",
          "A comprehensive dashboard showing key performance indicators for the data engineering "
              + "team including pipeline success rates, data freshness SLAs, cost per query across "
              + "BigQuery and Snowflake, and alerting thresholds for anomaly detection on "
              + "critical business metrics.",
          "The user profile enrichment pipeline joins raw clickstream events from Kafka with "
              + "demographic data from Salesforce and purchase history from the data warehouse "
              + "to produce a unified customer 360 view used by the recommendation engine "
              + "and the marketing automation platform.",
          "Schema documentation for the payments fact table in the finance data mart. Contains "
              + "denormalized records of all payment transactions with foreign keys to the "
              + "customer dimension, product dimension, and date dimension tables. Partitioned "
              + "by transaction_date with a 90-day retention policy.");

  @Test
  public void runBenchmark() throws Exception {
    String modelDir = System.getProperty("ONNX_MODEL_DIR");
    if (modelDir == null || modelDir.isBlank()) {
      throw new org.testng.SkipException(
          "ONNX_MODEL_DIR not set. Run with -DONNX_MODEL_DIR=/path/to/model");
    }

    Path modelPath = Path.of(modelDir);
    if (!Files.isDirectory(modelPath)) {
      throw new org.testng.SkipException("ONNX_MODEL_DIR does not exist: " + modelDir);
    }

    System.out.println("=== ONNX Embedding Benchmark ===");
    System.out.println("Model directory: " + modelDir);
    System.out.println();

    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

    // --- Cold start measurement ---
    long heapBefore = memoryBean.getHeapMemoryUsage().getUsed();
    long rssBefore = getRssBytes();
    long coldStartBegin = System.nanoTime();

    OnnxEmbeddingProvider provider = new OnnxEmbeddingProvider(modelPath, 0, 0);

    long loadTimeNs = System.nanoTime() - coldStartBegin;

    // First inference (includes any lazy initialization)
    long firstInferenceBegin = System.nanoTime();
    float[] firstEmbedding = provider.embed("warmup query", null);
    long firstInferenceNs = System.nanoTime() - firstInferenceBegin;

    long heapAfter = memoryBean.getHeapMemoryUsage().getUsed();
    long rssAfter = getRssBytes();

    long coldStartTotalNs = loadTimeNs + firstInferenceNs;

    System.out.printf("Cold start (model load): %.1f ms%n", loadTimeNs / 1e6);
    System.out.printf("Cold start (first inference): %.1f ms%n", firstInferenceNs / 1e6);
    System.out.printf("Cold start (total): %.1f ms%n", coldStartTotalNs / 1e6);
    System.out.printf("Embedding dimensions: %d%n", firstEmbedding.length);
    System.out.printf("Heap delta: %.1f MB%n", (heapAfter - heapBefore) / (1024.0 * 1024.0));
    System.out.printf("RSS delta: %.1f MB%n", (rssAfter - rssBefore) / (1024.0 * 1024.0));
    System.out.println();

    // --- Warm latency measurement ---
    int iterations = 100;
    System.out.printf("Running %d warm iterations (mixed short + long queries)...%n", iterations);

    // Warmup (5 extra iterations not counted)
    for (int i = 0; i < 5; i++) {
      provider.embed(SHORT_QUERIES.get(i % SHORT_QUERIES.size()), null);
    }

    long[] latenciesNs = new long[iterations];
    for (int i = 0; i < iterations; i++) {
      String query;
      if (i % 3 == 0) {
        query = LONG_QUERIES.get(i % LONG_QUERIES.size());
      } else {
        query = SHORT_QUERIES.get(i % SHORT_QUERIES.size());
      }

      long start = System.nanoTime();
      provider.embed(query, null);
      latenciesNs[i] = System.nanoTime() - start;
    }

    Arrays.sort(latenciesNs);
    double meanMs = Arrays.stream(latenciesNs).average().orElse(0) / 1e6;
    double p50Ms = latenciesNs[iterations / 2] / 1e6;
    double p99Ms = latenciesNs[(int) (iterations * 0.99)] / 1e6;
    double minMs = latenciesNs[0] / 1e6;
    double maxMs = latenciesNs[iterations - 1] / 1e6;

    System.out.printf("Warm latency (mean): %.2f ms%n", meanMs);
    System.out.printf("Warm latency (p50):  %.2f ms%n", p50Ms);
    System.out.printf("Warm latency (p99):  %.2f ms%n", p99Ms);
    System.out.printf("Warm latency (min):  %.2f ms%n", minMs);
    System.out.printf("Warm latency (max):  %.2f ms%n", maxMs);

    long peakHeap = memoryBean.getHeapMemoryUsage().getUsed();
    long peakRss = getRssBytes();
    System.out.println();
    System.out.printf("Peak heap: %.1f MB%n", peakHeap / (1024.0 * 1024.0));
    System.out.printf("Peak RSS: %.1f MB%n", peakRss / (1024.0 * 1024.0));

    // --- Write report ---
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode report = mapper.createObjectNode();
    report.put("model", modelPath.getFileName().toString());
    report.put("modelDir", modelDir);
    report.put("embeddingDimensions", firstEmbedding.length);

    ObjectNode coldStart = report.putObject("coldStart");
    coldStart.put("modelLoadMs", loadTimeNs / 1e6);
    coldStart.put("firstInferenceMs", firstInferenceNs / 1e6);
    coldStart.put("totalMs", coldStartTotalNs / 1e6);

    ObjectNode warmLatency = report.putObject("warmLatency");
    warmLatency.put("iterations", iterations);
    warmLatency.put("meanMs", meanMs);
    warmLatency.put("p50Ms", p50Ms);
    warmLatency.put("p99Ms", p99Ms);
    warmLatency.put("minMs", minMs);
    warmLatency.put("maxMs", maxMs);

    ObjectNode memory = report.putObject("memory");
    memory.put("heapDeltaMB", (heapAfter - heapBefore) / (1024.0 * 1024.0));
    memory.put("rssDeltaMB", (rssAfter - rssBefore) / (1024.0 * 1024.0));
    memory.put("peakHeapMB", peakHeap / (1024.0 * 1024.0));
    memory.put("peakRssMB", peakRss / (1024.0 * 1024.0));

    ArrayNode sampleEmbedding = report.putArray("sampleEmbeddingFirst5");
    for (int i = 0; i < Math.min(5, firstEmbedding.length); i++) {
      sampleEmbedding.add(firstEmbedding[i]);
    }

    String reportJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report);

    Path reportPath = Path.of("build", "onnx-benchmark-report.json");
    Files.createDirectories(reportPath.getParent());
    Files.writeString(reportPath, reportJson);

    System.out.println();
    System.out.println("Report written to: " + reportPath.toAbsolutePath());
    System.out.println();
    System.out.println(reportJson);

    provider.close();
  }

  private static long getRssBytes() {
    try {
      long pid = ProcessHandle.current().pid();
      Process process =
          new ProcessBuilder("ps", "-o", "rss=", "-p", String.valueOf(pid))
              .redirectErrorStream(true)
              .start();
      String output = new String(process.getInputStream().readAllBytes()).trim();
      process.waitFor();
      // ps reports RSS in kilobytes
      return Long.parseLong(output) * 1024;
    } catch (IOException | InterruptedException | NumberFormatException e) {
      return -1;
    }
  }
}
