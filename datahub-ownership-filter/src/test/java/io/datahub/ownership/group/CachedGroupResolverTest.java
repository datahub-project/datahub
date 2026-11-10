package io.datahub.ownership.group;

import com.github.benmanes.caffeine.cache.Ticker;
import com.linkedin.common.urn.Urn;
import com.datahub.authentication.group.GroupService;
import io.datahubproject.metadata.context.OperationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class CachedGroupResolverTest {

    private GroupService groupService;
    private OperationContext opCtx;
    private AtomicLong nanos;
    private Ticker fakeTicker;
    private CachedGroupResolver resolver;

    private final Urn alice = urn("urn:li:corpuser:alice");
    private final Urn bob = urn("urn:li:corpuser:bob");
    private final Urn g1 = urn("urn:li:corpGroup:g1");
    private final Urn g2 = urn("urn:li:corpGroup:g2");

    static Urn urn(String s) { try { return Urn.createFromString(s); } catch (Exception e) { throw new RuntimeException(e); } }

    @BeforeEach
    void setUp() throws Exception {
        groupService = mock(GroupService.class);
        opCtx = mock(OperationContext.class);
        nanos = new AtomicLong(0L);
        fakeTicker = nanos::get;
        resolver = new CachedGroupResolver(groupService, fakeTicker, 5, TimeUnit.MINUTES);

        when(groupService.getGroupsForUser(any(), eq(alice))).thenReturn(List.of(g1, g2));
        when(groupService.getGroupsForUser(any(), eq(bob))).thenReturn(List.of(g1));
    }

    @Test
    void cachesPerUser() throws Exception {
        assertThat(resolver.groupsFor(opCtx, alice)).containsExactlyInAnyOrder(g1, g2);
        assertThat(resolver.groupsFor(opCtx, alice)).containsExactlyInAnyOrder(g1, g2);
        verify(groupService, times(1)).getGroupsForUser(any(), eq(alice));
    }

    @Test
    void differentUsersGetDifferentEntries() throws Exception {
        assertThat(resolver.groupsFor(opCtx, alice)).hasSize(2);
        assertThat(resolver.groupsFor(opCtx, bob)).hasSize(1);
        verify(groupService, times(1)).getGroupsForUser(any(), eq(alice));
        verify(groupService, times(1)).getGroupsForUser(any(), eq(bob));
    }

    @Test
    void refetchesAfterTtl() throws Exception {
        resolver.groupsFor(opCtx, alice);
        nanos.addAndGet(TimeUnit.MINUTES.toNanos(5) + 1);
        resolver.groupsFor(opCtx, alice);
        verify(groupService, times(2)).getGroupsForUser(any(), eq(alice));
    }

    @Test
    void doesNotCacheExceptions() throws Exception {
        when(groupService.getGroupsForUser(any(), eq(alice)))
            .thenThrow(new RuntimeException("boom"))
            .thenReturn(List.of(g1));

        try { resolver.groupsFor(opCtx, alice); } catch (Exception ignored) {}
        assertThat(resolver.groupsFor(opCtx, alice)).containsExactly(g1);
        verify(groupService, times(2)).getGroupsForUser(any(), eq(alice));
    }
}
