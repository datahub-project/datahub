package com.linkedin.dataset.client;

import com.linkedin.common.client.DatasetsClient;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.RestliRemoteDAO;
import com.linkedin.metadata.dao.internal.RestliRemoteWriterDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.restli.client.Client;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class DatasetSnapshots extends DatasetsClient {
  private static final Set<Class<? extends RecordTemplate>> ASPECTS =
      ModelUtils.getValidAspectTypes(DatasetAspect.class);

  private final RestliRemoteDAO<DatasetSnapshot, DatasetAspect, DatasetUrn> _remoteDAO;
  private final RestliRemoteWriterDAO _remoteWriterDAO;

  public DatasetSnapshots(@Nonnull Client restliClient) {
    super(restliClient);
    this._remoteDAO = new RestliRemoteDAO<>(DatasetSnapshot.class, DatasetAspect.class, restliClient);
    this._remoteWriterDAO = new RestliRemoteWriterDAO(restliClient);
  }

  @Nonnull
  public DatasetSnapshot getLatestFullSnapshot(@Nonnull DatasetUrn datasetUrn) throws Exception {

    Map<Class<? extends RecordTemplate>, Optional<? extends RecordTemplate>> resp = _remoteDAO.get(ASPECTS, datasetUrn);

    List<? extends UnionTemplate> aspects = resp.values()
        .stream()
        .filter(Optional::isPresent)
        .map(aspect -> ModelUtils.newAspectUnion(DatasetAspect.class, aspect.get()))
        .collect(Collectors.toList());

    return ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn, aspects);
  }

  @Nonnull
  public <ASPECT extends RecordTemplate> Optional<ASPECT> getAspect(@Nonnull DatasetUrn datasetUrn,
      @Nonnull Class<ASPECT> aspect, long version) {
    AspectKey<DatasetUrn, ASPECT> aspectKey = new AspectKey<>(aspect, datasetUrn, version);
    return (Optional<ASPECT>) _remoteDAO.get(Collections.singleton(aspectKey)).get(aspectKey);
  }

  public void createSnapshot(@Nonnull DatasetUrn datasetUrn, @Nonnull DatasetSnapshot snapshot) throws Exception {
    _remoteWriterDAO.create(datasetUrn, snapshot);
  }
}
