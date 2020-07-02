package com.linkedin.metadata.dao;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.producer.DummyMetadataEventProducer;
import com.linkedin.metadata.dao.utils.RecordUtils;
import io.ebean.Ebean;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


/**
 * An immutable implementation of {@link BaseLocalDAO}. Suitable for serving statically declared metadata.
 */
public class ImmutableLocalDAO<ASPECT_UNION extends UnionTemplate, URN extends Urn> extends EbeanLocalDAO<ASPECT_UNION, URN> {

  private static final JSONParser JSON_PARSER = new JSONParser();

  private static final AuditStamp DUMMY_AUDIT_STAMP = new AuditStamp().setActor(new CorpuserUrn("unknown")).setTime(0L);

  private static final String GMA_CREATE_ALL_SQL = "gma-create-all.sql";

  /**
   * Constructs an {@link ImmutableLocalDAO} from a hard-coded URN-Aspect map
   */
  public ImmutableLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Map<URN, ? extends RecordTemplate> urnAspectMap) {

    super(aspectUnionClass, new DummyMetadataEventProducer(),
        createProductionH2ServerConfig(aspectUnionClass.getCanonicalName()));
    _server.execute(Ebean.createSqlUpdate(readSQLfromFile(GMA_CREATE_ALL_SQL)));
    urnAspectMap.forEach((key, value) -> super.save(key, value, DUMMY_AUDIT_STAMP, LATEST_VERSION, true));
  }

  // For testing purpose
  public ImmutableLocalDAO(@Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull Map<URN, ? extends RecordTemplate> urnAspectMap, boolean ddlGenerate) {

    super(aspectUnionClass, new DummyMetadataEventProducer(), createTestingH2ServerConfig());
    urnAspectMap.forEach((key, value) -> super.save(key, value, DUMMY_AUDIT_STAMP, LATEST_VERSION, true));
  }

  /**
   * Loads a map of URN to aspect values from an {@link InputStream}.
   *
   * The InputStream is expected to contain a JSON map where the keys are a specific type of URN and values are a
   * specific type of metadata aspect.
   */
  @Nonnull
  public static <URN extends Urn, ASPECT extends RecordTemplate> Map<URN, ASPECT> loadAspects(
      @Nonnull Class<ASPECT> aspectClass, @Nonnull InputStream inputStream)
      throws IOException, ParseException, URISyntaxException {

    final Map<URN, ASPECT> aspects = new HashMap<>();
    try (InputStreamReader reader = new InputStreamReader(inputStream, Charset.defaultCharset())) {
      JSONObject root = (JSONObject) JSON_PARSER.parse(reader);
      for (Map.Entry entry : (Set<Map.Entry>) root.entrySet()) {
        URN urn = (URN) Urn.createFromString((String) entry.getKey());
        ASPECT aspect = RecordUtils.toRecordTemplate(aspectClass, entry.getValue().toString());
        aspects.put(urn, aspect);
      }
    }

    return aspects;
  }

  @Override
  @Nonnull
  public <ASPECT extends RecordTemplate> ASPECT add(@Nonnull URN urn, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> updateLambda, @Nonnull AuditStamp auditStamp,
      int maxTransactionRetry) {
    throw new UnsupportedOperationException("Not supported by immutable DAO");
  }

  @Override
  public long newNumericId() {
    throw new UnsupportedOperationException("Not supported by immutable DAO");
  }

  @Nonnull
  private String readSQLfromFile(@Nonnull String resourcePath) {
    try {
      return Resources.toString(Resources.getResource(resourcePath), Charsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
