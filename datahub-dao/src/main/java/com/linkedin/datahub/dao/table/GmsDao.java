package com.linkedin.datahub.dao.table;

import com.linkedin.dataplatform.client.DataPlatforms;
import com.linkedin.dataset.client.Datasets;
import com.linkedin.dataset.client.Deprecations;
import com.linkedin.dataset.client.InstitutionalMemory;
import com.linkedin.dataset.client.Lineages;
import com.linkedin.dataset.client.Ownerships;
import com.linkedin.dataset.client.Schemas;
import com.linkedin.identity.client.CorpUsers;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.restli.client.Client;
import javax.annotation.Nonnull;
import lombok.Getter;


@Getter
public class GmsDao {

  private final CorpUsers _corpUsers;
  private final Datasets _datasets;
  private final Ownerships _ownerships;
  private final InstitutionalMemory _institutionalMemory;
  private final Deprecations _deprecations;
  private final Schemas _schemas;
  private final Lineages _lineages;
  private final DataPlatforms _dataPlatforms;

  public GmsDao(@Nonnull Client restClient) {
    _corpUsers = new CorpUsers(restClient);
    _datasets = new Datasets(restClient);
    _ownerships = new Ownerships(restClient);
    _institutionalMemory = new InstitutionalMemory(restClient);
    _deprecations = new Deprecations(restClient);
    _schemas = new Schemas(restClient);
    _lineages = new Lineages(restClient);
    _dataPlatforms = new DataPlatforms(restClient);
  }

  public GmsDao(@Nonnull String restliHostName, @Nonnull int restliHostPort) {
    this(DefaultRestliClientFactory.getRestLiClient(restliHostName, restliHostPort));
  }
}
