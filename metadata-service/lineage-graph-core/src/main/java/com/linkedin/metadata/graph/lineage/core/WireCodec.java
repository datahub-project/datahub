package com.linkedin.metadata.graph.lineage.core;

import static com.linkedin.metadata.graph.lineage.core.WalkRecords.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Binary encoding of a walk result ({@code DHLG} v1), shared by the server and the GMS client.
 *
 * <p>Paths are not sent as lists. Each relationship carries the parent pointer of its stored path,
 * and the client rebuilds paths exactly as {@link WalkResult#paths} does. URNs are sent once, in a
 * front-coded table (URNs share long prefixes), and referenced by index. All integers are LEB128
 * varints; times are zigzag varints.
 *
 * <p>The header's watermark is the snapshot's build time. The edge timestamp, actor and manual
 * fields are still in the format for the decoder but are never set: the graph does not keep them.
 *
 * <pre>
 * "DHLG" version:u8 status:u8 partial:u8 hops edgesScanned walkMicros watermark lagMillis
 * relTypeCount { len bytes }*
 * urnCount { shared suffixLen suffix }*
 * relCount {
 *   urn relType degree degree2+1 flags
 *   [hasStored: parent (relationship index if parentHadStored, else urn) via+1]
 *   kinds(kind1 | kind2 << 2) [arg1] [arg2]
 *   [createdOn] [updatedOn] createdActor+1 updatedActor+1
 * }*
 * </pre>
 *
 * <p>A lineage search answered on the server ({@code DHLS} v1) is either the page and counts, or a
 * {@code DHLG} body for the full walk when the graph-only search does not apply:
 *
 * <pre>
 * "DHLS" version:u8 mode:u8
 * FULL:      prefiltered searchMicros DHLG...
 * LIGHTNING: status:u8 partial:u8 hops edgesScanned walkMicros searchMicros watermark lagMillis
 *            walked prefiltered total
 *            stringCount { shared suffixLen suffix }*       (sorted, front-coded)
 *            facetCount { valueCount { string count }* countedAfterLastNew:u8 }*
 *            rowCount { string degree pathCount { length { string }* }* }*
 * </pre>
 */
public final class WireCodec {

  private static final byte[] MAGIC = {'D', 'H', 'L', 'G'};
  private static final int VERSION = 1;
  private static final byte[] SEARCH_MAGIC = {'D', 'H', 'L', 'S'};
  private static final int SEARCH_VERSION = 1;

  private static final int W_EXPLORED = 1;
  private static final int W_MANUAL = 2;
  private static final int W_HAS_STORED = 4;
  private static final int W_PARENT_HAD_STORED = 8;
  private static final int W_CREATED = 16;
  private static final int W_UPDATED = 32;

  private WireCodec() {}

  /** Encodes {@code r} and returns the number of bytes written. */
  public static long encode(WalkResult r, long walkMicros, long lagMillis, OutputStream out)
      throws IOException {
    GraphSnapshot g = r.graph();
    int n = r.size();

    // Record -> relationship index, for parent and path references.
    int maxRec = -1;
    for (int i = 0; i < n; i++) {
      maxRec = Math.max(maxRec, r.record(i));
    }
    int[] resOf = new int[maxRec + 1];
    Arrays.fill(resOf, -1);
    for (int i = 0; i < n; i++) {
      resOf[r.record(i)] = i;
    }

    // Every URN the response mentions, sent once in node-id order (byte order for base nodes).
    int[] ids = new int[n * 3];
    int k = 0;
    for (int i = 0; i < n; i++) {
      int rc = r.record(i);
      ids[k++] = r.field(rc, NODE);
      int flags = r.field(rc, FLAGS);
      if ((flags & F_HAS_STORED) != 0) {
        if ((flags & F_PARENT_HAD_STORED) == 0) {
          ids[k++] = r.field(rc, S_PARENT_NODE);
        }
        if (r.field(rc, S_VIA) >= 0) {
          ids[k++] = r.field(rc, S_VIA);
        }
      }
    }
    Arrays.sort(ids, 0, k);
    int u = 0;
    for (int i = 0; i < k; i++) {
      if (u == 0 || ids[u - 1] != ids[i]) {
        ids[u++] = ids[i];
      }
    }
    int[] urnIds = Arrays.copyOf(ids, u);

    Buf b = new Buf(Math.max(1024, n * 24));
    b.bytes(MAGIC);
    b.u8(VERSION);
    b.u8(r.status().ordinal());
    b.u8(r.partial() ? 1 : 0);
    b.varint(r.hops());
    b.varint(r.edgesScanned());
    b.varint(walkMicros);
    b.varint(g.builtAtMillis());
    b.varint(Math.max(0, lagMillis));

    List<String> rels = g.relTypes().names();
    b.varint(rels.size());
    for (String rel : rels) {
      byte[] s = rel.getBytes(StandardCharsets.UTF_8);
      b.varint(s.length);
      b.bytes(s);
    }

    b.varint(urnIds.length);
    UrnDictionary.Cursor cursor = g.base.urns.cursor();
    byte[] prev = new byte[0];
    for (int id : urnIds) {
      String urn = cursor.get(id);
      byte[] cur = urn.getBytes(StandardCharsets.UTF_8);
      int shared = Arrays.mismatch(prev, cur);
      if (shared < 0) {
        shared = cur.length;
      }
      shared = Math.min(shared, Math.min(prev.length, cur.length));
      b.varint(shared);
      b.varint(cur.length - shared);
      b.bytes(cur, shared, cur.length - shared);
      prev = cur;
    }

    b.varint(n);
    for (int i = 0; i < n; i++) {
      int rc = r.record(i);
      int flags = r.field(rc, FLAGS);
      int packed = r.field(rc, PACKED);
      int wf = 0;
      if ((flags & F_EXPLORED) != 0) {
        wf |= W_EXPLORED;
      }
      if ((flags & F_HAS_STORED) != 0) {
        wf |= W_HAS_STORED;
        if ((flags & F_PARENT_HAD_STORED) != 0) {
          wf |= W_PARENT_HAD_STORED;
        }
      }
      b.varint(local(urnIds, r.field(rc, NODE)));
      b.varint(WalkRecords.relType(packed));
      b.varint(r.field(rc, DEGREE));
      b.varint(r.field(rc, DEGREE2) + 1L);
      b.u8(wf);
      if ((wf & W_HAS_STORED) != 0) {
        b.varint(
            (wf & W_PARENT_HAD_STORED) != 0
                ? resOf[r.field(rc, S_PARENT_REC)]
                : local(urnIds, r.field(rc, S_PARENT_NODE)));
        int via = r.field(rc, S_VIA);
        b.varint(via < 0 ? 0 : local(urnIds, via) + 1L);
      }
      int k1 = kind1(packed);
      int k2 = kind2(packed);
      b.varint(k1 | (k2 << 2));
      if (k1 != KIND_NONE) {
        b.varint(resOf[r.field(rc, ARG1)]);
      }
      if (k2 != KIND_NONE) {
        b.varint(resOf[r.field(rc, ARG2)]);
      }
      // No edge timestamps, actors or manual flag: the graph does not keep them.
      b.varint(0);
      b.varint(0);
    }
    out.write(b.data, 0, b.size);
    return b.size;
  }

  private static int local(int[] sortedIds, int id) {
    return Arrays.binarySearch(sortedIds, id);
  }

  /** Decodes a response, rebuilding every relationship's paths. */
  public static ImpactResponse decode(byte[] data) {
    return decode(new In(data));
  }

  private static ImpactResponse decode(In in) {
    for (byte m : MAGIC) {
      if (in.u8() != (m & 0xFF)) {
        throw new IllegalArgumentException("Not a DHLG response");
      }
    }
    int version = in.u8();
    if (version != VERSION) {
      throw new IllegalArgumentException("Unsupported DHLG version " + version);
    }
    WalkResult.Status status = WalkResult.Status.values()[in.u8()];
    boolean partial = in.u8() != 0;
    int hops = (int) in.varint();
    long scanned = in.varint();
    long walkMicros = in.varint();
    long watermark = in.varint();
    long lag = in.varint();

    String[] rels = new String[(int) in.varint()];
    for (int i = 0; i < rels.length; i++) {
      rels[i] = in.string((int) in.varint());
    }
    String[] urns = new String[(int) in.varint()];
    byte[] prev = new byte[0];
    for (int i = 0; i < urns.length; i++) {
      int shared = (int) in.varint();
      int suffix = (int) in.varint();
      byte[] cur = Arrays.copyOf(prev, shared + suffix);
      in.bytes(cur, shared, suffix);
      urns[i] = new String(cur, StandardCharsets.UTF_8);
      prev = cur;
    }

    int n = (int) in.varint();
    int[] node = new int[n];
    int[] parent = new int[n];
    int[] via = new int[n];
    int[] wflags = new int[n];
    int[] kinds = new int[n];
    int[] arg1 = new int[n];
    int[] arg2 = new int[n];
    List<ImpactResponse.Relationship> out = new ArrayList<>(n);
    String[] type = new String[n];
    int[] degree = new int[n];
    int[][] degrees = new int[n][];
    Long[] created = new Long[n];
    Long[] updated = new Long[n];
    String[] createdBy = new String[n];
    String[] updatedBy = new String[n];
    for (int i = 0; i < n; i++) {
      node[i] = (int) in.varint();
      type[i] = rels[(int) in.varint()];
      degree[i] = (int) in.varint();
      int d2 = (int) in.varint() - 1;
      degrees[i] = d2 < 0 ? new int[] {degree[i]} : new int[] {degree[i], d2};
      int wf = in.u8();
      wflags[i] = wf;
      via[i] = -1;
      if ((wf & W_HAS_STORED) != 0) {
        parent[i] = (int) in.varint();
        via[i] = (int) in.varint() - 1;
      }
      int kk = (int) in.varint();
      kinds[i] = kk;
      if ((kk & 3) != KIND_NONE) {
        arg1[i] = (int) in.varint();
      }
      if (((kk >>> 2) & 3) != KIND_NONE) {
        arg2[i] = (int) in.varint();
      }
      if ((wf & W_CREATED) != 0) {
        created[i] = Varint.unzigzag(in.varint());
      }
      if ((wf & W_UPDATED) != 0) {
        updated[i] = Varint.unzigzag(in.varint());
      }
      int ca = (int) in.varint() - 1;
      int ua = (int) in.varint() - 1;
      createdBy[i] = ca < 0 ? null : urns[ca];
      updatedBy[i] = ua < 0 ? null : urns[ua];
    }

    PathBuilder paths = new PathBuilder(urns, node, parent, via, wflags);
    for (int i = 0; i < n; i++) {
      List<List<String>> p = new ArrayList<>(2);
      paths.add(p, kinds[i] & 3, arg1[i], node[i]);
      paths.add(p, (kinds[i] >>> 2) & 3, arg2[i], node[i]);
      out.add(
          new ImpactResponse.Relationship(
              urns[node[i]],
              type[i],
              degree[i],
              degrees[i],
              (wflags[i] & W_EXPLORED) != 0,
              (wflags[i] & W_MANUAL) != 0,
              created[i],
              createdBy[i],
              updated[i],
              updatedBy[i],
              p));
    }
    return new ImpactResponse(status, partial, hops, scanned, walkMicros, watermark, lag, out);
  }

  /** Encodes a search answered with the full walk; returns the number of bytes written. */
  public static long encodeSearchFull(
      WalkResult r,
      long walkMicros,
      long searchMicros,
      long lagMillis,
      int prefiltered,
      OutputStream out)
      throws IOException {
    Buf b = new Buf(32);
    b.bytes(SEARCH_MAGIC);
    b.u8(SEARCH_VERSION);
    b.u8(ImpactSearchResponse.Mode.FULL.ordinal());
    b.varint(prefiltered);
    b.varint(searchMicros);
    out.write(b.data, 0, b.size);
    return b.size + encode(r, walkMicros, lagMillis, out);
  }

  /** Encodes a lightning search page; returns the number of bytes written. */
  public static long encodeSearch(ImpactSearchResponse s, OutputStream out) throws IOException {
    Map<String, Integer> index = new HashMap<>();
    for (ImpactSearchResponse.Facet f : s.facets()) {
      f.values().forEach(v -> index.put(v, 0));
    }
    for (ImpactSearchResponse.Row row : s.rows()) {
      index.put(row.urn(), 0);
      row.paths().forEach(p -> p.forEach(u -> index.put(u, 0)));
    }
    String[] strings = index.keySet().toArray(new String[0]);
    Arrays.sort(strings);
    for (int i = 0; i < strings.length; i++) {
      index.put(strings[i], i);
    }

    Buf b = new Buf(1024);
    b.bytes(SEARCH_MAGIC);
    b.u8(SEARCH_VERSION);
    b.u8(ImpactSearchResponse.Mode.LIGHTNING.ordinal());
    b.u8(s.status().ordinal());
    b.u8(s.partial() ? 1 : 0);
    b.varint(s.hops());
    b.varint(s.edgesScanned());
    b.varint(s.walkMicros());
    b.varint(s.searchMicros());
    b.varint(s.watermark());
    b.varint(Math.max(0, s.lagMillis()));
    b.varint(s.walked());
    b.varint(s.prefiltered());
    b.varint(s.total());
    b.varint(strings.length);
    byte[] prev = new byte[0];
    for (String str : strings) {
      byte[] cur = str.getBytes(StandardCharsets.UTF_8);
      int shared = Arrays.mismatch(prev, cur);
      if (shared < 0) {
        shared = cur.length;
      }
      shared = Math.min(shared, Math.min(prev.length, cur.length));
      b.varint(shared);
      b.varint(cur.length - shared);
      b.bytes(cur, shared, cur.length - shared);
      prev = cur;
    }
    b.varint(s.facets().size());
    for (ImpactSearchResponse.Facet f : s.facets()) {
      b.varint(f.values().size());
      for (int i = 0; i < f.values().size(); i++) {
        b.varint(index.get(f.values().get(i)));
        b.varint(f.counts()[i]);
      }
      b.u8(f.countedAfterLastNew() ? 1 : 0);
    }
    b.varint(s.rows().size());
    for (ImpactSearchResponse.Row row : s.rows()) {
      b.varint(index.get(row.urn()));
      b.varint(row.degree());
      b.varint(row.paths().size());
      for (List<String> p : row.paths()) {
        b.varint(p.size());
        for (String u : p) {
          b.varint(index.get(u));
        }
      }
    }
    out.write(b.data, 0, b.size);
    return b.size;
  }

  public static ImpactSearchResponse decodeSearch(byte[] data) {
    In in = new In(data);
    for (byte m : SEARCH_MAGIC) {
      if (in.u8() != (m & 0xFF)) {
        throw new IllegalArgumentException("Not a DHLS response");
      }
    }
    int version = in.u8();
    if (version != SEARCH_VERSION) {
      throw new IllegalArgumentException("Unsupported DHLS version " + version);
    }
    ImpactSearchResponse.Mode mode = ImpactSearchResponse.Mode.values()[in.u8()];
    if (mode == ImpactSearchResponse.Mode.FULL) {
      int prefiltered = (int) in.varint();
      long searchMicros = in.varint();
      ImpactResponse full = decode(in);
      return new ImpactSearchResponse(
          mode,
          full,
          full.status(),
          full.partial(),
          full.hops(),
          full.edgesScanned(),
          full.walkMicros(),
          searchMicros,
          full.watermark(),
          full.lagMillis(),
          full.relationships().size(),
          prefiltered,
          0,
          List.of(),
          List.of());
    }
    WalkResult.Status status = WalkResult.Status.values()[in.u8()];
    boolean partial = in.u8() != 0;
    int hops = (int) in.varint();
    long scanned = in.varint();
    long walkMicros = in.varint();
    long searchMicros = in.varint();
    long watermark = in.varint();
    long lag = in.varint();
    int walked = (int) in.varint();
    int prefiltered = (int) in.varint();
    int total = (int) in.varint();
    String[] strings = new String[(int) in.varint()];
    byte[] prev = new byte[0];
    for (int i = 0; i < strings.length; i++) {
      int shared = (int) in.varint();
      int suffix = (int) in.varint();
      byte[] cur = Arrays.copyOf(prev, shared + suffix);
      in.bytes(cur, shared, suffix);
      strings[i] = new String(cur, StandardCharsets.UTF_8);
      prev = cur;
    }
    int facetCount = (int) in.varint();
    List<ImpactSearchResponse.Facet> facets = new ArrayList<>(facetCount);
    for (int f = 0; f < facetCount; f++) {
      int n = (int) in.varint();
      List<String> values = new ArrayList<>(n);
      long[] counts = new long[n];
      for (int i = 0; i < n; i++) {
        values.add(strings[(int) in.varint()]);
        counts[i] = in.varint();
      }
      facets.add(new ImpactSearchResponse.Facet(values, counts, in.u8() != 0));
    }
    int rowCount = (int) in.varint();
    List<ImpactSearchResponse.Row> rows = new ArrayList<>(rowCount);
    for (int r = 0; r < rowCount; r++) {
      String urn = strings[(int) in.varint()];
      int degree = (int) in.varint();
      int pathCount = (int) in.varint();
      List<List<String>> paths = new ArrayList<>(pathCount);
      for (int p = 0; p < pathCount; p++) {
        int len = (int) in.varint();
        List<String> path = new ArrayList<>(len);
        for (int k = 0; k < len; k++) {
          path.add(strings[(int) in.varint()]);
        }
        paths.add(path);
      }
      rows.add(new ImpactSearchResponse.Row(urn, degree, paths));
    }
    return new ImpactSearchResponse(
        mode,
        null,
        status,
        partial,
        hops,
        scanned,
        walkMicros,
        searchMicros,
        watermark,
        lag,
        walked,
        prefiltered,
        total,
        facets,
        rows);
  }

  /** Rebuilds stored paths from parent pointers, sharing each parent's path. */
  private static final class PathBuilder {
    private final String[] urns;
    private final int[] node;
    private final int[] parent;
    private final int[] via;
    private final int[] wflags;
    private final List<String>[] stored;

    @SuppressWarnings("unchecked")
    PathBuilder(String[] urns, int[] node, int[] parent, int[] via, int[] wflags) {
      this.urns = urns;
      this.node = node;
      this.parent = parent;
      this.via = via;
      this.wflags = wflags;
      this.stored = new List[node.length];
    }

    void add(List<List<String>> out, int kind, int arg, int selfUrn) {
      List<String> p;
      if (kind == KIND_STORED) {
        p = stored(arg);
      } else if (kind == KIND_VIA_OF) {
        List<String> child = stored(arg);
        String self = urns[selfUrn];
        int cut = child.indexOf(self);
        p = cut < 0 ? child : child.subList(0, cut + 1);
      } else {
        return;
      }
      if (!out.contains(p)) {
        out.add(p);
      }
    }

    List<String> stored(int i) {
      if (stored[i] != null) {
        return stored[i];
      }
      // Walk up to the nearest relationship whose path is already built, then build down.
      List<Integer> chain = new ArrayList<>();
      int cur = i;
      while (stored[cur] == null) {
        chain.add(cur);
        if ((wflags[cur] & W_PARENT_HAD_STORED) == 0) {
          break;
        }
        cur = parent[cur];
      }
      for (int j = chain.size() - 1; j >= 0; j--) {
        int c = chain.get(j);
        List<String> p;
        if ((wflags[c] & W_PARENT_HAD_STORED) != 0) {
          p = new ArrayList<>(stored[parent[c]].size() + 2);
          p.addAll(stored[parent[c]]);
        } else {
          p = new ArrayList<>(3);
          p.add(urns[parent[c]]);
        }
        if (via[c] >= 0) {
          p.add(urns[via[c]]);
        }
        p.add(urns[node[c]]);
        stored[c] = p;
      }
      return stored[i];
    }
  }

  private static final class Buf {
    byte[] data;
    int size;

    Buf(int capacity) {
      data = new byte[capacity];
    }

    private void ensure(int n) {
      if (size + n > data.length) {
        data = Arrays.copyOf(data, Math.max(data.length * 2, size + n));
      }
    }

    void u8(int v) {
      ensure(1);
      data[size++] = (byte) v;
    }

    void varint(long v) {
      ensure(10);
      size = Varint.write(data, size, v);
    }

    void bytes(byte[] b) {
      bytes(b, 0, b.length);
    }

    void bytes(byte[] b, int off, int len) {
      ensure(len);
      System.arraycopy(b, off, data, size, len);
      size += len;
    }
  }

  private static final class In {
    private final byte[] data;
    private int pos;

    In(byte[] data) {
      this.data = data;
    }

    int u8() {
      return data[pos++] & 0xFF;
    }

    long varint() {
      long result = 0;
      int shift = 0;
      byte b;
      do {
        b = data[pos++];
        result |= (long) (b & 0x7F) << shift;
        shift += 7;
      } while (b < 0);
      return result;
    }

    String string(int len) {
      String s = new String(data, pos, len, StandardCharsets.UTF_8);
      pos += len;
      return s;
    }

    void bytes(byte[] dst, int off, int len) {
      System.arraycopy(data, pos, dst, off, len);
      pos += len;
    }
  }
}
