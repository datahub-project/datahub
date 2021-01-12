package com.linkedin.datahub.dao.table;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.util.CorpUserUtil;
import com.linkedin.identity.CorpUser;
import com.linkedin.identity.client.CorpUsers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class CorpUsersDao {

    private final CorpUsers _corpUsers;

    public CorpUsersDao(@Nonnull CorpUsers corpUsers) {
        _corpUsers = corpUsers;
    }

    @Nonnull
    public List<CorpUser> getCorpUsers(@Nonnull List<String> corpUserUrnStrs) throws Exception {
        List<CorpuserUrn> corpUserUrns = corpUserUrnStrs
                .stream()
                .map(this::getOwnerUrn)
                .collect(Collectors.toList());

        Map<CorpuserUrn, CorpUser> corpUserMap = _corpUsers.batchGet(new HashSet<>(corpUserUrns));

        List<CorpUser> results = new ArrayList<>();
        for (CorpuserUrn urn : corpUserUrns) {
            results.add(corpUserMap.getOrDefault(urn, null));
        }
        return results;
    }

    @Nonnull
    public CorpUser getCorpUser(@Nonnull String corpUserUrn) throws Exception {
        CorpuserUrn corpUser = getOwnerUrn(corpUserUrn);
        return corpUser == null ? null : _corpUsers.get(corpUser);
    }


    @Nonnull
    private Set<CorpuserUrn> getOwnerUrns(@Nonnull Set<String> corpUserUrns) {
        return corpUserUrns.stream()
                .map(this::getOwnerUrn)
                .collect(Collectors.toSet());
    }

    @Nullable
    private CorpuserUrn getOwnerUrn(@Nonnull String corpUserUrn) {
        try {
            return CorpUserUtil.toCorpUserUrn(corpUserUrn);
        } catch (URISyntaxException e) {
            return null;
        }
    }
}
