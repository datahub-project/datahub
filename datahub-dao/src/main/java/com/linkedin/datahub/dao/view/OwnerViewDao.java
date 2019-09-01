package com.linkedin.datahub.dao.view;

import com.linkedin.common.Owner;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.datahub.models.view.DatasetOwner;
import com.linkedin.datahub.models.view.DatasetOwnership;
import com.linkedin.datahub.util.CorpUserUtil;
import com.linkedin.datahub.util.OwnerUtil;
import com.linkedin.dataset.client.Ownerships;
import com.linkedin.identity.CorpUser;
import com.linkedin.identity.client.CorpUsers;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.util.DatasetUtil.*;

@Slf4j
public class OwnerViewDao {
  private final Ownerships _ownerships;
  private final CorpUsers _corpUsers;

  public OwnerViewDao(@Nonnull Ownerships ownerships, @Nonnull CorpUsers corpUsers) {
    _ownerships = ownerships;
    _corpUsers = corpUsers;
  }

  @Nonnull
  public DatasetOwnership getDatasetOwners(@Nonnull String datasetUrn) throws Exception {
    final DatasetUrn urn = toDatasetUrn(datasetUrn);

    Ownership ownership = _ownerships.getLatestOwnership(urn);
    Map<CorpuserUrn, CorpUser> owners = _corpUsers.batchGet(getOwnerUrns(ownership));

    DatasetOwnership datasetOwnership = new DatasetOwnership();
    datasetOwnership.setDatasetUrn(datasetUrn);
    datasetOwnership.setFromUpstream(false);
    datasetOwnership.setOwners(fillDatasetOwner(ownership, owners));
    datasetOwnership.setActor(ownership.getLastModified().getActor().getContent());
    datasetOwnership.setLastModified(ownership.getLastModified().getTime());
    return datasetOwnership;
  }

  @Nonnull
  private List<DatasetOwner> fillDatasetOwner(@Nonnull Ownership ownership,
      @Nonnull Map<CorpuserUrn, CorpUser> owners) {
    final Long modified = ownership.getLastModified().getTime();
    return ownership.getOwners()
        .stream()
        .map(o -> OwnerUtil.toWhOwner(o, owners.get(getOwnerUrn(o))))
        .peek(o -> o.setModifiedTime(modified))
        .collect(Collectors.toList());
  }

  @Nullable
  private CorpuserUrn getOwnerUrn(@Nonnull Owner owner) {
    try {
      return CorpUserUtil.toCorpUserUrn(owner.getOwner().toString());
    } catch (URISyntaxException e) {
      log.error("CorpuserUrn syntax error", e);
      return null;
    }
  }

  @Nonnull
  private Set<CorpuserUrn> getOwnerUrns(@Nonnull Ownership ownership) {
    return ownership.getOwners().stream()
        .map(this::getOwnerUrn)
        .collect(Collectors.toSet());
  }
}
