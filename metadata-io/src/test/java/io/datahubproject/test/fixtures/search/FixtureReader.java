package io.datahubproject.test.fixtures.search;

import static io.datahubproject.test.fixtures.search.SearchFixtureUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import lombok.Builder;
import lombok.NonNull;
import org.apache.commons.io.FilenameUtils;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;

@Builder
public class FixtureReader {
  @Builder.Default private String inputBase = SearchFixtureUtils.FIXTURE_BASE;
  @NonNull private ESBulkProcessor bulkProcessor;
  @NonNull private String fixtureName;
  @Builder.Default private String targetIndexPrefix = "";

  private long refreshIntervalSeconds;

  public Set<String> read() throws IOException {
    try (Stream<Path> files =
        Files.list(Paths.get(String.format("%s/%s", inputBase, fixtureName)))) {
      return files
          .map(
              file -> {
                String absolutePath = file.toAbsolutePath().toString();
                String indexName =
                    String.format(
                        "%s_%s",
                        targetIndexPrefix,
                        FilenameUtils.getBaseName(absolutePath).split("[.]", 2)[0]);

                try (Stream<String> lines = getLines(absolutePath)) {
                  lines.forEach(
                      line -> {
                        try {
                          UrnDocument doc = OBJECT_MAPPER.readValue(line, UrnDocument.class);
                          IndexRequest request =
                              new IndexRequest(indexName)
                                  .id(doc.urn)
                                  .source(line.getBytes(), XContentType.JSON);

                          bulkProcessor.add(request);
                        } catch (JsonProcessingException e) {
                          throw new RuntimeException(e);
                        }
                      });
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }

                return indexName;
              })
          .collect(Collectors.toSet());
    } finally {
      bulkProcessor.flush();
      try {
        Thread.sleep(1000 * refreshIntervalSeconds);
      } catch (InterruptedException ignored) {
      }
    }
  }

  private Stream<String> getLines(String path) throws IOException {
    if (FilenameUtils.getExtension(path).equals("gz")) {
      return GZIPFiles.lines(Paths.get(path));
    } else {
      return Files.lines(Paths.get(path));
    }
  }

  public static class GZIPFiles {
    /**
     * Get a lazily loaded stream of lines from a gzipped file, similar to {@link
     * Files#lines(java.nio.file.Path)}.
     *
     * @param path The path to the gzipped file.
     * @return stream with lines.
     */
    public static Stream<String> lines(Path path) {
      InputStream fileIs = null;
      BufferedInputStream bufferedIs = null;
      GZIPInputStream gzipIs = null;
      try {
        fileIs = Files.newInputStream(path);
        // Even though GZIPInputStream has a buffer it reads individual bytes
        // when processing the header, better add a buffer in-between
        bufferedIs = new BufferedInputStream(fileIs, 65535);
        gzipIs = new GZIPInputStream(bufferedIs);
      } catch (IOException e) {
        closeSafely(gzipIs);
        closeSafely(bufferedIs);
        closeSafely(fileIs);
        throw new UncheckedIOException(e);
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(gzipIs));
      return reader.lines().onClose(() -> closeSafely(reader));
    }

    private static void closeSafely(Closeable closeable) {
      if (closeable != null) {
        try {
          closeable.close();
        } catch (IOException e) {
          // Ignore
        }
      }
    }
  }

  public static class UrnDocument {
    public String urn;
  }
}
