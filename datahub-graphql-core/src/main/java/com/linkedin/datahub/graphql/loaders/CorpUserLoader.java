package com.linkedin.datahub.graphql.loaders;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpUser;
import com.linkedin.identity.client.CorpUsers;
import org.dataloader.BatchLoader;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Responsible for fetching {@link CorpUser} objects from the downstream GMS, leveraging
 * the public clients.
 */
public class CorpUserLoader implements BatchLoader<String, CorpUser> {

    public static final String NAME = "corpUserLoader";

    private final CorpUsers _corpUsersClient;

    public CorpUserLoader(final CorpUsers corpUsersClient) {
        _corpUsersClient = corpUsersClient;
    }

    @Override
    public CompletionStage<List<CorpUser>> load(List<String> keys) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                final List<CorpuserUrn> corpUserUrns = keys
                        .stream()
                        .map(this::getCorpUserUrn)
                        .collect(Collectors.toList());

                final Map<CorpuserUrn, CorpUser> corpUserMap = _corpUsersClient
                        .batchGet(new HashSet<>(corpUserUrns));

                final List<CorpUser> results = new ArrayList<>();
                for (CorpuserUrn urn : corpUserUrns) {
                    results.add(corpUserMap.getOrDefault(urn, null));
                }
                return results;
            } catch (Exception e) {
                throw new RuntimeException("Failed to batch load CorpUsers", e);
            }
        });
    }

    private CorpuserUrn getCorpUserUrn(String urnStr) {
        try {
            return CorpuserUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve user with urn %s, invalid urn", urnStr));
        }
    }
}
