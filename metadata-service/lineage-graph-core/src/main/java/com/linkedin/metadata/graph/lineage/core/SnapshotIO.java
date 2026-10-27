package com.linkedin.metadata.graph.lineage.core;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Snapshot files: the base graph's off-heap segments written byte for byte, so loading is a memory
 * map rather than a rebuild.
 *
 * <p>Layout under a snapshot root:
 *
 * <pre>
 * root/LATEST              name of the newest complete snapshot directory
 * root/&lt;builtAtMillis&gt;/     segments (*.bin), type tables (*.txt), meta.properties
 * </pre>
 *
 * <p>The builder writes a snapshot under a temporary name, renames it into place, then replaces
 * {@code LATEST} with a rename, so a reader never sees a partial snapshot. The previous snapshot is
 * kept for rollback; older ones are deleted. A replica that still maps a deleted one keeps reading
 * it until it unmaps it.
 */
public final class SnapshotIO {

  static final String FORMAT = "2";
  public static final String LATEST = "LATEST";
  private static final int KEEP = 2;

  private SnapshotIO() {}

  /**
   * What the builder records with a snapshot.
   *
   * @param builtAtMillis when the build finished
   * @param pointInTimeMillis when the scan's point in time was opened
   * @param stats extra counters (scanned documents, duplicates), stored as strings
   */
  public record Meta(long builtAtMillis, long pointInTimeMillis, Map<String, String> stats) {}

  /** A mapped snapshot. */
  public record Loaded(BaseGraph graph, Meta meta, Path dir) {}

  /**
   * Writes {@code g} as a new snapshot under {@code root} and makes it the latest. Returns its
   * directory.
   */
  public static Path publish(BaseGraph g, Meta meta, Path root) throws IOException {
    Files.createDirectories(root);
    String name = Long.toString(meta.builtAtMillis());
    Path dir = root.resolve(name);
    Path tmp = root.resolve(name + ".tmp");
    if (Files.exists(tmp)) {
      deleteRecursively(tmp);
    }
    Files.createDirectories(tmp);
    MemorySegment[] segs = g.segments();
    for (int i = 0; i < segs.length; i++) {
      writeSegment(tmp.resolve(BaseGraph.SEGMENTS[i] + ".bin"), segs[i]);
    }
    writeSegment(tmp.resolve("urnData.bin"), g.urns.data());
    writeSegment(tmp.resolve("urnBlocks.bin"), g.urns.blockOffsets());
    writeLines(tmp.resolve("entityTypes.txt"), g.entityTypes.names());
    writeLines(tmp.resolve("relTypes.txt"), g.relTypes.names());
    writeLines(tmp.resolve("platforms.txt"), g.platforms.names());
    writeLines(tmp.resolve("environments.txt"), g.environments.names());
    Properties p = new Properties();
    meta.stats().forEach(p::setProperty);
    p.setProperty("format", FORMAT);
    p.setProperty("builtAt", Long.toString(meta.builtAtMillis()));
    p.setProperty("pointInTime", Long.toString(meta.pointInTimeMillis()));
    p.setProperty("nodes", Integer.toString(g.nodeCount()));
    p.setProperty("edges", Integer.toString(g.edgeCount()));
    p.setProperty("urns", Integer.toString(g.urns.size()));
    try (Writer w = Files.newBufferedWriter(tmp.resolve("meta.properties"))) {
      p.store(w, null);
    }
    Files.move(tmp, dir, StandardCopyOption.ATOMIC_MOVE);

    Path latestTmp = root.resolve(LATEST + ".tmp");
    Files.writeString(latestTmp, name + "\n");
    Files.move(latestTmp, root.resolve(LATEST), StandardCopyOption.ATOMIC_MOVE);
    prune(root, name);
    return dir;
  }

  /** The directory {@code LATEST} names, if any. */
  public static Optional<Path> latest(Path root) throws IOException {
    Path pointer = root.resolve(LATEST);
    if (!Files.isRegularFile(pointer)) {
      return Optional.empty();
    }
    String name = Files.readString(pointer).trim();
    return name.isEmpty() ? Optional.empty() : Optional.of(root.resolve(name));
  }

  /** Memory-maps a snapshot into {@code arena}; closing the arena unmaps it. Pages load lazily. */
  public static Loaded load(Path dir, Arena arena) throws IOException {
    Properties p = new Properties();
    try (Reader r = Files.newBufferedReader(dir.resolve("meta.properties"))) {
      p.load(r);
    }
    if (!FORMAT.equals(p.getProperty("format"))) {
      throw new IOException(
          "Unsupported snapshot format " + p.getProperty("format") + " in " + dir);
    }
    MemorySegment[] segs = new MemorySegment[BaseGraph.SEGMENTS.length];
    for (int i = 0; i < segs.length; i++) {
      segs[i] = map(dir.resolve(BaseGraph.SEGMENTS[i] + ".bin"), arena);
    }
    UrnDictionary urns =
        UrnDictionary.of(
            Integer.parseInt(p.getProperty("urns")),
            map(dir.resolve("urnData.bin"), arena),
            map(dir.resolve("urnBlocks.bin"), arena));
    BaseGraph g =
        new BaseGraph(
            urns,
            TypeTable.of(
                "entity type", BaseGraph.MAX_ENTITY_TYPES, true, readLines(dir, "entityTypes.txt")),
            TypeTable.of(
                "relationship type", BaseGraph.MAX_REL_TYPES, true, readLines(dir, "relTypes.txt")),
            TypeTable.of(
                "platform", BaseGraph.MAX_PLATFORMS, false, readLines(dir, "platforms.txt")),
            TypeTable.of(
                "environment",
                BaseGraph.MAX_ENVIRONMENTS,
                false,
                readLines(dir, "environments.txt")),
            Integer.parseInt(p.getProperty("nodes")),
            Integer.parseInt(p.getProperty("edges")),
            segs);
    Map<String, String> stats = new LinkedHashMap<>();
    p.stringPropertyNames().stream().sorted().forEach(k -> stats.put(k, p.getProperty(k)));
    Meta meta =
        new Meta(
            Long.parseLong(p.getProperty("builtAt")),
            Long.parseLong(p.getProperty("pointInTime")),
            stats);
    return new Loaded(g, meta, dir);
  }

  /** Bytes of a snapshot directory on disk. */
  public static long sizeOnDisk(Path dir) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      long total = 0;
      for (Path f : files.toList()) {
        total += Files.size(f);
      }
      return total;
    }
  }

  private static MemorySegment map(Path file, Arena arena) throws IOException {
    try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
      return ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size(), arena);
    }
  }

  /** Deletes every snapshot but the newest {@link #KEEP}, never {@code latest}. */
  private static void prune(Path root, String latest) throws IOException {
    List<Path> dirs;
    try (Stream<Path> s = Files.list(root)) {
      dirs =
          s.filter(Files::isDirectory)
              .filter(d -> d.getFileName().toString().matches("\\d+"))
              .sorted(
                  Comparator.comparingLong((Path d) -> Long.parseLong(d.getFileName().toString()))
                      .reversed())
              .toList();
    }
    for (int i = KEEP; i < dirs.size(); i++) {
      if (!dirs.get(i).getFileName().toString().equals(latest)) {
        deleteRecursively(dirs.get(i));
      }
    }
  }

  private static void writeSegment(Path file, MemorySegment seg) throws IOException {
    try (FileChannel ch =
        FileChannel.open(
            file,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      long chunk = 1 << 26;
      for (long pos = 0; pos < seg.byteSize(); pos += chunk) {
        ByteBuffer buf = seg.asSlice(pos, Math.min(chunk, seg.byteSize() - pos)).asByteBuffer();
        while (buf.hasRemaining()) {
          ch.write(buf);
        }
      }
      ch.force(false);
    }
  }

  private static void writeLines(Path file, List<String> lines) throws IOException {
    Files.write(file, lines, StandardCharsets.UTF_8);
  }

  private static List<String> readLines(Path dir, String name) throws IOException {
    return Files.readAllLines(dir.resolve(name), StandardCharsets.UTF_8);
  }

  private static void deleteRecursively(Path dir) throws IOException {
    try (Stream<Path> paths = Files.walk(dir)) {
      for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
        Files.delete(p);
      }
    }
  }
}
