package com.linkedin.metadata.kafka.hook;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.UpdateIndicesHookTest.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class MCLProcessingTestDataGenerator {

  private MCLProcessingTestDataGenerator() {}

  public static MetadataChangeLog createBaseChangeLog() throws URISyntaxException {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(Constants.DATASET_ENTITY_NAME);
    event.setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    UpstreamLineage upstreamLineage = createBaseLineageAspect();

    event.setAspect(GenericRecordUtils.serializeAspect(upstreamLineage));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(
        new AuditStamp().setActor(UrnUtils.getUrn(TEST_ACTOR_URN)).setTime(EVENT_TIME));
    return event;
  }

  public static MetadataChangeLog setToRestate(MetadataChangeLog changeLog) {
    return changeLog.setChangeType(ChangeType.RESTATE);
  }

  public static MetadataChangeLog setToUpsert(MetadataChangeLog changeLog) {
    return changeLog.setChangeType(ChangeType.UPSERT);
  }

  public static MetadataChangeLog setSystemMetadata(MetadataChangeLog changeLog) {
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setRunId(RUN_ID_1);
    systemMetadata.setLastObserved(LAST_OBSERVED_1);
    return changeLog.setSystemMetadata(systemMetadata);
  }

  public static MetadataChangeLog setSystemMetadataWithForceIndexing(MetadataChangeLog changeLog) {
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setRunId(RUN_ID_1);
    systemMetadata.setLastObserved(LAST_OBSERVED_1);
    StringMap stringMap = new StringMap();
    stringMap.put(FORCE_INDEXING_KEY, Boolean.TRUE.toString());
    systemMetadata.setProperties(stringMap);
    return changeLog.setSystemMetadata(systemMetadata);
  }

  public static MetadataChangeLog setPreviousData(
      MetadataChangeLog changeLog, MetadataChangeLog previousState) {
    changeLog.setPreviousAspectValue(previousState.getAspect());
    return changeLog.setPreviousSystemMetadata(previousState.getSystemMetadata());
  }

  public static MetadataChangeLog setPreviousDataToEmpty(MetadataChangeLog changeLog) {
    changeLog.removePreviousAspectValue();
    changeLog.removePreviousSystemMetadata();
    return changeLog;
  }

  public static MetadataChangeLog modifySystemMetadata(MetadataChangeLog changeLog) {
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setRunId(RUN_ID_2);
    systemMetadata.setLastObserved(LAST_OBSERVED_2);
    return changeLog.setSystemMetadata(systemMetadata);
  }

  public static MetadataChangeLog modifySystemMetadata2(MetadataChangeLog changeLog) {
    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setRunId(RUN_ID_3);
    systemMetadata.setLastObserved(LAST_OBSERVED_3);
    return changeLog.setSystemMetadata(systemMetadata);
  }

  public static MetadataChangeLog modifyAspect(
      MetadataChangeLog changeLog, UpstreamLineage upstreamLineage) {
    return changeLog.setAspect(GenericRecordUtils.serializeAspect(upstreamLineage));
  }

  public static UpstreamLineage createBaseLineageAspect() throws URISyntaxException {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    final UpstreamArray upstreamArray = new UpstreamArray();
    final Upstream upstream = new Upstream();
    upstream.setType(DatasetLineageType.TRANSFORMED);
    upstream.setDataset(DatasetUrn.createFromString(TEST_DATASET_URN_2));
    upstreamArray.add(upstream);
    upstreamLineage.setUpstreams(upstreamArray);

    return upstreamLineage;
  }

  public static UpstreamLineage addLineageEdge(UpstreamLineage upstreamLineage)
      throws URISyntaxException {
    UpstreamArray upstreamArray = upstreamLineage.getUpstreams();
    Upstream upstream = new Upstream();
    upstream.setType(DatasetLineageType.TRANSFORMED);
    upstream.setDataset(DatasetUrn.createFromString(TEST_DATASET_URN_3));
    upstreamArray.add(upstream);
    return upstreamLineage.setUpstreams(upstreamArray);
  }

  public static UpstreamLineage modifyNonSearchableField(UpstreamLineage upstreamLineage) {
    UpstreamArray upstreamArray = upstreamLineage.getUpstreams();
    Upstream upstream = upstreamArray.get(0);
    Map<String, String> stringMap = new HashMap<>();
    stringMap.put("key", "value");
    upstream.setProperties(new StringMap(stringMap));
    upstreamArray.set(0, upstream);
    return upstreamLineage.setUpstreams(upstreamArray);
  }
}
