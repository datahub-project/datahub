package io.datahub.ownership.group;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.linkedin.common.urn.Urn;
import com.datahub.authentication.group.GroupService;
import io.datahubproject.metadata.context.OperationContext;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CachedGroupResolver {

    private final GroupService groupService;
    private final Cache<Urn, List<Urn>> cache;

    public CachedGroupResolver(@Nonnull GroupService groupService) {
        this(groupService, Ticker.systemTicker(), 5, TimeUnit.MINUTES);
    }

    public CachedGroupResolver(
            @Nonnull GroupService groupService,
            @Nonnull Ticker ticker,
            long expireAfterWriteAmount,
            @Nonnull TimeUnit expireAfterWriteUnit) {
        this.groupService = groupService;
        this.cache = Caffeine.newBuilder()
            .ticker(ticker)
            .expireAfterWrite(expireAfterWriteAmount, expireAfterWriteUnit)
            .maximumSize(50_000)
            .build();
    }

    @Nonnull
    public List<Urn> groupsFor(@Nonnull OperationContext opCtx, @Nonnull Urn userUrn) throws Exception {
        List<Urn> cached = cache.getIfPresent(userUrn);
        if (cached != null) return cached;
        List<Urn> fresh = groupService.getGroupsForUser(opCtx, userUrn);
        cache.put(userUrn, fresh);
        return fresh;
    }

    public void invalidate(@Nonnull Urn userUrn) {
        cache.invalidate(userUrn);
    }
}
