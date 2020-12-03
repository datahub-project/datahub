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
 * This class holds method for taking a snapshot to generate relevant documents.
 */
@Slf4j
public final class SnapshotProcessor {

  /**
   * Mapping of metadata snapshot type to the list of document index builders that subscribe to it.
   */
  private final Map<String, List<BaseIndexBuilder<?>>> _snapshotTypeToIndexBuilders;

  /**
   * Constructor.
   *
   * @param builders Set of document index builders who are interested in parsing metadata snapshot
   */
  public SnapshotProcessor(@Nonnull Set<? extends BaseIndexBuilder> builders) {
    _snapshotTypeToIndexBuilders = new HashMap<>();

    for (BaseIndexBuilder<?> builder : builders) {
      List<Class<? extends RecordTemplate>> snapshotsSubscribed = builder._snapshotsInterested;
      snapshotsSubscribed.forEach(snapshot -> {
        _snapshotTypeToIndexBuilders.putIfAbsent(snapshot.getName(), new ArrayList<>());
        _snapshotTypeToIndexBuilders.get(snapshot.getName()).add(builder);
      });
    }
  }

  /**
   * Constructs documents to update from a snapshot.
   *
   * <p>Given a snapshot which is a union of metadata snapshot types, this function returns list of documents to update
   * from parsing non-empty metadata aspects.
   *
   * <p>A given metadata snapshot type will contain metadata aspects, each such aspect could be used by multiple
   * document index builders to construct a document.
   *
   * <p>Each document index builder will subscribe to certain snapshot types whose aspects they are interested in, by
   * providing the list of snapshot types in function snapshotsInterested()
   *
   * <p>Each document index builder will parse relevant aspects from a metadata snapshot type it has subscribed to and
   * return documents to update.
   *
   * @param snapshot Snapshot from which the document needs to be parsed
   * @return List of documents
   */
  @Nonnull
  public List<RecordTemplate> getDocumentsToUpdate(@Nonnull Snapshot snapshot) {
    final List<RecordTemplate> docsList = new ArrayList<>();
    final DataMap snapshotData = (DataMap) snapshot.data();
    for (String clazz : snapshotData.keySet()) {
      Class<? extends RecordTemplate> snapshotClass = ModelUtils.getMetadataSnapshotClassFromName(clazz);
      if (!_snapshotTypeToIndexBuilders.containsKey(clazz)) {
        continue;
      }
      final List<? extends BaseIndexBuilder<?>> builders = _snapshotTypeToIndexBuilders.get(clazz);
      for (BaseIndexBuilder<?> builder : builders) {
        try {
          final Object obj = snapshotClass.getConstructor(DataMap.class).newInstance((DataMap) snapshotData.get(clazz));
          List<? extends RecordTemplate> records = builder.getDocumentsToUpdate((RecordTemplate) obj);
          docsList.addAll(records);
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
          log.error("Failed to get documents due to error ", e);
        }
      }
    }
    return docsList;
  }
}
