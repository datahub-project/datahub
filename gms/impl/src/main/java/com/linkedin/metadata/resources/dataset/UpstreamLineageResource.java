package com.linkedin.metadata.resources.dataset;

import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.dataset.UpstreamLineageDelta;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import javax.annotation.Nonnull;


/**
 * Rest.li entry point: /datasets/{datasetKey}/upstreamLineage
 */
@RestLiCollection(name = "upstreamLineage", namespace = "com.linkedin.dataset", parent = Datasets.class)
public final class UpstreamLineageResource extends BaseDatasetVersionedAspectResource<UpstreamLineage> {

    public UpstreamLineageResource() {
        super(UpstreamLineage.class);
    }

    @Nonnull
    @Override
    @RestMethod.Get
    public Task<UpstreamLineage> get(@Nonnull Long version) {
        return super.get(version);
    }

    @Nonnull
    @Override
    @RestMethod.Create
    public Task<CreateResponse> create(@Nonnull UpstreamLineage upstreamLineage) {
        return super.create(upstreamLineage);
    }

    @Nonnull
    @Action(name = "deltaUpdate")
    public UpstreamLineage deltaUpdate(@ActionParam("delta") @Nonnull UpstreamLineageDelta delta) {
        final UpstreamLineage upstreamLineage = new UpstreamLineage();

        super.create(UpstreamLineage.class, optionalOldUpstreamLineage -> {
            final LinkedHashSet<Upstream> upstreams = new LinkedHashSet<>();
            final UpstreamLineage oldUpstreamLineage = (UpstreamLineage) optionalOldUpstreamLineage.orElse(null);
            if (oldUpstreamLineage != null) {
                upstreams.addAll(new ArrayList<>(oldUpstreamLineage.getUpstreams()));
            }

            upstreams.addAll(delta.getUpstreamsToUpdate());

            upstreamLineage.setUpstreams(new UpstreamArray(upstreams));
            return upstreamLineage;
        });

        return upstreamLineage;
    }
}