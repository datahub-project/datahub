package com.linkedin.datahub.dao.table;

import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.models.view.DatasetOwner;
import com.linkedin.dataset.client.Ownerships;
import java.util.List;
import javax.annotation.Nonnull;

import static com.linkedin.datahub.util.DatasetUtil.*;
import static com.linkedin.datahub.util.OwnerUtil.*;

public class DatasetOwnerDao {
    private final Ownerships _ownerships;

    public DatasetOwnerDao(@Nonnull Ownerships ownerships) {
        this._ownerships = ownerships;
    }

    public void updateDatasetOwners(@Nonnull String datasetUrn, @Nonnull List<DatasetOwner> owners,
                                    @Nonnull String user) throws Exception {
        OwnerArray ownerArray = new OwnerArray();
        for (DatasetOwner owner : owners) {
            ownerArray.add(toTmsOwner(owner));
        }
        _ownerships.createOwnership(toDatasetUrn(datasetUrn), new Ownership().setOwners(ownerArray),
                new CorpuserUrn(user));
    }
}
