package com.linkedin.metadata.builders.search;

import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.Snapshot;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * This class holds method for taking a snapshot to generate relevant documents
 */

@Slf4j
public final class SnapshotProcessor {

  // Set of document index builders that are interested in parsing the snapshot
  private final Set<? extends BaseIndexBuilder> _registeredBuilders;

  /**
   * Constructor
   *
   * @param registerdBuilders Set of document index builders who are interested in parsing metadata snapshot
   */
  public SnapshotProcessor(@Nonnull Set<? extends BaseIndexBuilder> registerdBuilders) {
    _registeredBuilders = registerdBuilders;
  }

  /**
   * Constructs mapping of metadata snapshot type to the list of document index builders that subscribe to it.
   */
  @Nonnull
  private Map<String, List<Class<? extends BaseIndexBuilder>>> getSnapshotBuildersMap() {
    Map<String, List<Class<? extends BaseIndexBuilder>>> snapshotBuilderMap = new HashMap<>();
    for (BaseIndexBuilder builder : _registeredBuilders) {
      @SuppressWarnings("unchecked")
      List<Class<? extends RecordTemplate>> snapshotsSubscribed = builder._snapshotsInterested;
      snapshotsSubscribed.forEach(snapshot -> {
        snapshotBuilderMap.putIfAbsent(snapshot.getName(), new ArrayList<>());
        snapshotBuilderMap.get(snapshot.getName()).add(builder.getClass());
      });
    }
    return snapshotBuilderMap;
  }

  /**
   * Constructs documents to update from a snapshot.
   *
   * <p> Given a snapshot which is a union of metadata snapshot types, this function returns list of documents to update
   * from parsing non-empty metadata aspects.
   *
   * <p> A given metadata snapshot type will contain metadata aspects, each such aspect could be used by multiple
   * document index builders to construct a document.
   *
   * <p> Each document index builder will subscribe to certain snapshot types whose aspects they are interested in, by
   * providing the list of snapshot types in function snapshotsInterested()
   *
   * <p> Each document index builder will parse relevant aspects from a metadata snapshot type it has subscribed to and
   * return documents to update.
   *
   * @param snapshot Snapshot from which the document needs to be parsed
   * @return List of documents
   */
  @Nonnull
  public List<RecordTemplate> getDocumentsToUpdate(@Nonnull Snapshot snapshot) {
    Map<String, List<Class<? extends BaseIndexBuilder>>> snapshotBuilderMap = getSnapshotBuildersMap();
    List<RecordTemplate> docsList = new ArrayList<>();
    DataMap snapshotData = (DataMap) snapshot.data();
    for (String clazz : snapshotData.keySet()) {
      Class<? extends RecordTemplate> snapshotClass = ModelUtils.getMetadataSnapshotClassFromName(clazz);
      if (!snapshotBuilderMap.containsKey(clazz)) {
        continue;
      }
      List<Class<? extends BaseIndexBuilder>> builders = snapshotBuilderMap.get(clazz);
      for (Class<? extends BaseIndexBuilder> builderClass : builders) {
        try {
          Object obj = snapshotClass.getConstructor(DataMap.class).newInstance((DataMap) snapshotData.get(clazz));
          @SuppressWarnings("unchecked")
          List<? extends RecordTemplate> records =
              builderClass.getConstructor().newInstance().getDocumentsToUpdate((RecordTemplate) obj);
          docsList.addAll(records);
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
          log.error("Failed to get documents due to error ", e);
        }
      }
    }
    return docsList;
  }
}
