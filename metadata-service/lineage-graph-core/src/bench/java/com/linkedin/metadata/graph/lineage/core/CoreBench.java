package com.linkedin.metadata.graph.lineage.core;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Phase 0 bench: loads a lineage-bench dataset into the in-memory graph and reports
 *
 * <ol>
 *   <li>load time, split into CSV parse and each build step;
 *   <li>memory: exact off-heap bytes per structure, retained heap, and process RSS;
 *   <li>correctness: every start node's walk size against {@code expected_total};
 *   <li>latency per start node: the walk, decoding the result's URNs, and rebuilding all paths;
 *   <li>the gap + varint compression variant: size and raw BFS speed against plain int32.
 * </ol>
 *
 * <p>Usage: {@code CoreBench <dataset dir> [--repeats N] [--out results.json]}
 */
public final class CoreBench {

  private static final List<EdgeFilter.Rule> DATASET_DOWNSTREAM =
      List.of(
          new EdgeFilter.Rule(
              "dataset", CsvEdgeLoader.DOWNSTREAM_OF, EdgeFilter.Direction.INCOMING, "dataset"));

  private record StartNode(String urn, String family, String note, int expectedTotal) {}

  public static void main(String[] args) throws Exception {
    Path dir = Path.of(args[0]);
    int repeats = 10;
    Path out = null;
    Path publish = null;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("--repeats")) {
        repeats = Integer.parseInt(args[++i]);
      } else if (args[i].equals("--out")) {
        out = Path.of(args[++i]);
      } else if (args[i].equals("--publish")) {
        publish = Path.of(args[++i]);
      }
    }
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("dataset", dir.toAbsolutePath().toString());
    report.put("jvm", System.getProperty("java.vm.version"));
    report.put("cpus", Runtime.getRuntime().availableProcessors());

    long heapBefore = usedHeapAfterGc();
    long rssBefore = rssBytes();

    // 1. Load.
    long t0 = System.nanoTime();
    GraphBuilder builder = new GraphBuilder();
    CsvEdgeLoader.load(dir.resolve("edges.csv"), builder);
    long parseMs = (System.nanoTime() - t0) / 1_000_000;
    long builderHeap = builder.heapBytes();
    int rawEdges = builder.edgeCount();
    long t1 = System.nanoTime();
    BaseGraph graph = builder.build();
    long buildMs = (System.nanoTime() - t1) / 1_000_000;
    Map<String, Long> steps = new LinkedHashMap<>();
    steps.put("csvParse", parseMs);
    steps.putAll(builder.timingsMs());
    steps.put("total", parseMs + buildMs);
    builder = null;
    report.put("loadMs", steps);
    report.put("nodes", graph.nodeCount());
    report.put("edges", graph.edgeCount());
    report.put("edgesInFile", rawEdges);
    if (publish != null) {
      // A snapshot of the CSV graph for the service, when no OpenSearch holds the dataset.
      long now = System.currentTimeMillis();
      long tw = System.nanoTime();
      Path snap =
          SnapshotIO.publish(
              graph, new SnapshotIO.Meta(now, now, Map.of("source", dir.toString())), publish);
      report.put("snapshot", snap.toString());
      report.put("snapshotWriteMs", (System.nanoTime() - tw) / 1_000_000);
      report.put("snapshotBytes", SnapshotIO.sizeOnDisk(snap));
    }

    // 2. Memory.
    long heapAfter = usedHeapAfterGc();
    Map<String, Object> mem = new LinkedHashMap<>();
    Map<String, Long> offHeap = graph.memory();
    mem.put("offHeapBytes", offHeap);
    mem.put("retainedHeapBytes", Math.max(0, heapAfter - heapBefore));
    mem.put("builderHeapBytesPeakEstimate", builderHeap);
    mem.put("rssBytes", rssBytes());
    mem.put("committedHeapBytes", Runtime.getRuntime().totalMemory());
    mem.put("rssBytesAtStart", rssBefore);
    long perEdge = offHeap.get("neighbours") + offHeap.get("attributes") + offHeap.get("via");
    mem.put("bytesPerEdge", round2((double) perEdge / graph.edgeCount()));
    mem.put("bytesPerEdgeAll", round2((double) offHeap.get("total") / graph.edgeCount()));
    mem.put(
        "bytesPerUrnDictionary", round2((double) offHeap.get("urnDictionary") / graph.nodeCount()));
    mem.put("rawUrnBytes", rawUrnBytes(graph));
    report.put("memory", mem);

    // 3 + 4. Correctness and latency.
    List<StartNode> starts = readStartNodes(dir.resolve("start_nodes.csv"));
    ImpactWalker walker = new ImpactWalker(1);
    GraphSnapshot snapshot = GraphSnapshot.of(graph);
    EdgeFilter filter = EdgeFilter.compile(snapshot, DATASET_DOWNSTREAM);
    for (int pass = 0; pass < 3; pass++) {
      for (StartNode s : starts) {
        walker.walk(snapshot, walkRequest(s.urn()), filter).close();
      }
    }
    List<Map<String, Object>> walks = new ArrayList<>();
    int mismatches = 0;
    for (StartNode s : starts) {
      long[] walkNs = new long[repeats];
      long[] urnNs = new long[repeats];
      long[] pathNs = new long[repeats];
      int total = 0;
      int hops = 0;
      long scanned = 0;
      for (int r = 0; r < repeats; r++) {
        long a = System.nanoTime();
        WalkResult result = walker.walk(snapshot, walkRequest(s.urn()), filter);
        long b = System.nanoTime();
        decodeUrns(result);
        long c = System.nanoTime();
        rebuildPaths(result);
        long d = System.nanoTime();
        total = result.size();
        hops = result.hops();
        scanned = result.edgesScanned();
        result.close();
        walkNs[r] = b - a;
        urnNs[r] = c - b;
        pathNs[r] = d - c;
      }
      boolean ok = total == s.expectedTotal();
      if (!ok) {
        mismatches++;
      }
      Map<String, Object> w = new LinkedHashMap<>();
      w.put("family", s.family());
      w.put("note", s.note());
      w.put("expected", s.expectedTotal());
      w.put("total", total);
      w.put("ok", ok);
      w.put("hops", hops);
      w.put("edgesScanned", scanned);
      w.put("walkMsP50", ms(pct(walkNs, 50)));
      w.put("walkMsP99", ms(pct(walkNs, 99)));
      w.put("urnDecodeMsP50", ms(pct(urnNs, 50)));
      w.put("pathRebuildMsP50", ms(pct(pathNs, 50)));
      walks.add(w);
    }
    report.put("walks", walks);
    report.put("mismatches", mismatches);

    // 5. Compression variant.
    report.put("varint", varintVariant(graph, starts, repeats));

    printSummary(report);
    if (out != null) {
      Files.writeString(out, toJson(report, 0), StandardCharsets.UTF_8);
      System.out.println("\nWrote " + out.toAbsolutePath());
    }
    if (mismatches > 0) {
      System.exit(1);
    }
  }

  private static WalkRequest walkRequest(String urn) {
    return new WalkRequest(urn, DATASET_DOWNSTREAM, 1000, 0, true, null, null, 0);
  }

  private static Map<String, Object> varintVariant(
      BaseGraph g, List<StartNode> starts, int repeats) {
    Map<String, Object> v = new LinkedHashMap<>();
    try (Arena arena = Arena.ofConfined()) {
      long t = System.nanoTime();
      VarintAdjacency va = VarintAdjacency.reverseOf(g, arena);
      v.put("encodeMs", (System.nanoTime() - t) / 1_000_000);
      long int32Bytes = (long) g.edgeCount() * 4;
      v.put("reverseNeighboursInt32Bytes", int32Bytes);
      v.put("reverseNeighboursVarintBytes", va.dataBytes());
      v.put("varintBytesPerEdge", round2((double) va.dataBytes() / g.edgeCount()));
      long[] visited = new long[(g.nodeCount() + 63) >>> 6];
      int[] queue = new int[g.nodeCount()];
      List<Map<String, Object>> rows = new ArrayList<>();
      for (StartNode s : starts) {
        if (!s.note().equals("root")) {
          continue;
        }
        int root = g.urns().find(s.urn());
        long[] a = new long[repeats];
        long[] b = new long[repeats];
        int reachedA = 0;
        int reachedB = 0;
        for (int r = 0; r < repeats + 3; r++) {
          Arrays.fill(visited, 0);
          long x = System.nanoTime();
          reachedA = VarintAdjacency.bfsInt32(g, root, visited, queue);
          long y = System.nanoTime();
          Arrays.fill(visited, 0);
          long z = System.nanoTime();
          reachedB = va.bfs(root, visited, queue);
          long w = System.nanoTime();
          if (r >= 3) {
            a[r - 3] = y - x;
            b[r - 3] = w - z;
          }
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("family", s.family());
        row.put("reached", reachedA);
        row.put("sameResult", reachedA == reachedB);
        row.put("int32BfsMsP50", ms(pct(a, 50)));
        row.put("varintBfsMsP50", ms(pct(b, 50)));
        rows.add(row);
      }
      v.put("rawBfs", rows);
    }
    return v;
  }

  /** Decodes every result URN in id order, as a response writer would. */
  private static long decodeUrns(WalkResult r) {
    int[] ids = new int[r.size()];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = r.node(i);
    }
    Arrays.sort(ids);
    UrnDictionary.Cursor c = r.graph().base().urns().cursor();
    long bytes = 0;
    for (int id : ids) {
      bytes += c.get(id).length();
    }
    return bytes;
  }

  private static long rebuildPaths(WalkResult r) {
    long n = 0;
    for (int i = 0; i < r.size(); i++) {
      for (int[] p : r.paths(i)) {
        n += p.length;
      }
    }
    return n;
  }

  private static long rawUrnBytes(BaseGraph g) {
    UrnDictionary.Cursor c = g.urns().cursor();
    long total = 0;
    for (int i = 0; i < g.nodeCount(); i++) {
      total += c.get(i).getBytes(StandardCharsets.UTF_8).length;
    }
    return total;
  }

  private static List<StartNode> readStartNodes(Path csv) throws IOException {
    List<String> lines = Files.readAllLines(csv);
    List<String> header = splitCsv(lines.get(0));
    int urn = header.indexOf("urn");
    int family = header.indexOf("family");
    int note = header.indexOf("note");
    int total = header.indexOf("expected_total");
    List<StartNode> out = new ArrayList<>();
    for (String line : lines.subList(1, lines.size())) {
      if (line.isBlank()) {
        continue;
      }
      List<String> f = splitCsv(line);
      out.add(
          new StartNode(f.get(urn), f.get(family), f.get(note), Integer.parseInt(f.get(total))));
    }
    return out;
  }

  private static List<String> splitCsv(String line) {
    List<String> out = new ArrayList<>();
    StringBuilder cur = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);
      if (ch == '"') {
        quoted = !quoted;
      } else if (ch == ',' && !quoted) {
        out.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(ch);
      }
    }
    out.add(cur.toString());
    return out;
  }

  private static long usedHeapAfterGc() throws InterruptedException {
    Runtime rt = Runtime.getRuntime();
    for (int i = 0; i < 3; i++) {
      System.gc();
      Thread.sleep(200);
    }
    return rt.totalMemory() - rt.freeMemory();
  }

  private static long rssBytes() {
    try {
      Process p =
          new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(ProcessHandle.current().pid()))
              .start();
      String s = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
      p.waitFor();
      return Long.parseLong(s) * 1024;
    } catch (Exception e) {
      return -1;
    }
  }

  private static long pct(long[] v, int p) {
    long[] c = v.clone();
    Arrays.sort(c);
    return c[Math.min(c.length - 1, (int) Math.ceil(p / 100.0 * c.length) - 1)];
  }

  private static double ms(long ns) {
    return round2(ns / 1e6);
  }

  private static double round2(double d) {
    return Math.round(d * 100) / 100.0;
  }

  private static String mib(Object bytes) {
    return String.format(Locale.ROOT, "%.1f MiB", ((Number) bytes).longValue() / 1048576.0);
  }

  @SuppressWarnings("unchecked")
  private static void printSummary(Map<String, Object> r) {
    System.out.printf(
        "%nGraph: %,d nodes, %,d edges (%,d rows in edges.csv)%n",
        r.get("nodes"), r.get("edges"), r.get("edgesInFile"));
    System.out.println("\nLoad time (ms): " + r.get("loadMs"));
    Map<String, Object> mem = (Map<String, Object>) r.get("memory");
    System.out.println("\nMemory:");
    ((Map<String, Long>) mem.get("offHeapBytes"))
        .forEach((k, v) -> System.out.printf("  off-heap %-16s %s%n", k, mib(v)));
    System.out.printf("  raw URN strings         %s%n", mib(mem.get("rawUrnBytes")));
    System.out.printf("  retained heap           %s%n", mib(mem.get("retainedHeapBytes")));
    System.out.printf(
        "  builder heap (peak est) %s%n", mib(mem.get("builderHeapBytesPeakEstimate")));
    System.out.printf("  process RSS             %s%n", mib(mem.get("rssBytes")));
    System.out.printf("  committed JVM heap      %s%n", mib(mem.get("committedHeapBytes")));
    System.out.printf(
        "  bytes/edge %s (all bytes / edges %s); dictionary bytes/URN %s%n",
        mem.get("bytesPerEdge"), mem.get("bytesPerEdgeAll"), mem.get("bytesPerUrnDictionary"));
    System.out.printf(
        "%n%-14s %-8s %9s %9s %3s %5s %10s %10s %10s %10s%n",
        "family",
        "note",
        "expected",
        "total",
        "ok",
        "hops",
        "walk p50",
        "walk p99",
        "urns p50",
        "paths p50");
    for (Map<String, Object> w : (List<Map<String, Object>>) r.get("walks")) {
      System.out.printf(
          "%-14s %-8s %9d %9d %3s %5d %8.2fms %8.2fms %8.2fms %8.2fms%n",
          w.get("family"),
          w.get("note"),
          w.get("expected"),
          w.get("total"),
          (Boolean) w.get("ok") ? "y" : "N",
          w.get("hops"),
          w.get("walkMsP50"),
          w.get("walkMsP99"),
          w.get("urnDecodeMsP50"),
          w.get("pathRebuildMsP50"));
    }
    Map<String, Object> v = (Map<String, Object>) r.get("varint");
    System.out.printf(
        "%nReverse neighbours: int32 %s vs gap+varint %s (%s B/edge), encode %s ms%n",
        mib(v.get("reverseNeighboursInt32Bytes")),
        mib(v.get("reverseNeighboursVarintBytes")),
        v.get("varintBytesPerEdge"),
        v.get("encodeMs"));
    for (Map<String, Object> row : (List<Map<String, Object>>) v.get("rawBfs")) {
      System.out.printf(
          "  raw BFS %-14s reached %8d  int32 %7.2fms  varint %7.2fms  same=%s%n",
          row.get("family"),
          row.get("reached"),
          row.get("int32BfsMsP50"),
          row.get("varintBfsMsP50"),
          row.get("sameResult"));
    }
    System.out.println("\nMismatches: " + r.get("mismatches"));
  }

  @SuppressWarnings("unchecked")
  private static String toJson(Object o, int indent) {
    String pad = "  ".repeat(indent + 1);
    String end = "  ".repeat(indent);
    if (o instanceof Map<?, ?> m) {
      List<String> parts = new ArrayList<>();
      for (Map.Entry<String, Object> e : ((Map<String, Object>) m).entrySet()) {
        parts.add(pad + quote(e.getKey()) + ": " + toJson(e.getValue(), indent + 1));
      }
      return "{\n" + String.join(",\n", parts) + "\n" + end + "}";
    }
    if (o instanceof List<?> l) {
      List<String> parts = new ArrayList<>();
      for (Object x : l) {
        parts.add(pad + toJson(x, indent + 1));
      }
      return "[\n" + String.join(",\n", parts) + "\n" + end + "]";
    }
    if (o instanceof String s) {
      return quote(s);
    }
    return String.valueOf(o);
  }

  private static String quote(String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
