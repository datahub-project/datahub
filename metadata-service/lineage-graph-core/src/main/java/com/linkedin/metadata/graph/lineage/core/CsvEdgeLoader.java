package com.linkedin.metadata.graph.lineage.core;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Loads a lineage-bench {@code edges.csv} ({@code downstream_urn,upstream_urn}) as {@code
 * DownstreamOf} edges from the downstream dataset to the upstream one, the way DataHub stores
 * {@code UpstreamLineage}.
 *
 * <p>Parses bytes straight into the interner: no per-line Strings.
 */
public final class CsvEdgeLoader {

  public static final String DOWNSTREAM_OF = "DownstreamOf";

  private CsvEdgeLoader() {}

  public static void load(Path edgesCsv, GraphBuilder builder) throws IOException {
    load(edgesCsv, builder, 1 << 20);
  }

  static void load(Path edgesCsv, GraphBuilder builder, int bufferSize) throws IOException {
    int rel = builder.relType(DOWNSTREAM_OF);
    try (InputStream in = Files.newInputStream(edgesCsv)) {
      Parser p = new Parser(in, bufferSize);
      p.skipLine(); // header
      while (p.nextField()) {
        int downstream = builder.urn(p.buf, p.fieldStart, p.fieldLen);
        if (!p.nextField()) {
          throw new IOException("Line without an upstream_urn column in " + edgesCsv);
        }
        int upstream = builder.urn(p.buf, p.fieldStart, p.fieldLen);
        p.skipLine();
        builder.addEdge(downstream, rel, upstream);
      }
    }
  }

  /** Minimal CSV field reader: double-quoted fields may contain commas; no embedded quotes. */
  private static final class Parser {
    private final InputStream in;
    byte[] buf;
    private int pos;
    private int limit;
    int fieldStart;
    int fieldLen;

    Parser(InputStream in, int bufferSize) {
      this.in = in;
      this.buf = new byte[bufferSize];
    }

    /** Reads the next field of the current line; false at end of input. */
    boolean nextField() throws IOException {
      if (!ensure(1)) {
        return false;
      }
      if (buf[pos] == '\n' || buf[pos] == '\r') {
        skipLine();
        return nextField();
      }
      boolean quoted = buf[pos] == '"';
      int start = quoted ? pos + 1 : pos;
      int i = start;
      while (true) {
        if (i >= limit) {
          int shift = pos; // refill moves pos to 0
          boolean more = refill(pos);
          start -= shift;
          i -= shift;
          if (!more) {
            break;
          }
          continue;
        }
        byte b = buf[i];
        if (quoted ? b == '"' : (b == ',' || b == '\n' || b == '\r')) {
          break;
        }
        i++;
      }
      int end = i;
      pos = quoted ? i + 1 : i;
      if (pos >= limit) {
        // The separator is past the buffer: refill, keeping this field's bytes.
        int shift = start;
        refill(start);
        start -= shift;
        end -= shift;
      }
      if (pos < limit && buf[pos] == ',') {
        pos++;
      }
      fieldStart = start;
      fieldLen = end - start;
      return true;
    }

    void skipLine() throws IOException {
      while (ensure(1)) {
        if (buf[pos++] == '\n') {
          return;
        }
      }
    }

    private boolean ensure(int n) throws IOException {
      return limit - pos >= n || refill(pos);
    }

    /** Drops bytes before {@code keepFrom}, reads more, and returns false at end of input. */
    private boolean refill(int keepFrom) throws IOException {
      int keep = limit - keepFrom;
      if (keep == buf.length) {
        byte[] bigger = new byte[buf.length * 2];
        System.arraycopy(buf, keepFrom, bigger, 0, keep);
        buf = bigger;
      } else {
        System.arraycopy(buf, keepFrom, buf, 0, keep);
      }
      pos -= keepFrom;
      limit = keep;
      int r = in.read(buf, limit, buf.length - limit);
      if (r <= 0) {
        return false;
      }
      limit += r;
      return true;
    }
  }
}
