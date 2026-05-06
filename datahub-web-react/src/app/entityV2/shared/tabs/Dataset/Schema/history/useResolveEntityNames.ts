import { useMemo } from 'react';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntityDisplayNamesQuery } from '@graphql/timeline.generated';
import { ChangeTransaction, Entity } from '@types';

const URN_PREFIX = 'urn:li:';

/**
 * Collects all entity URNs referenced in timeline change events (from modifier
 * and parameters) and batch-resolves them to display names via a single GraphQL
 * query using the entityDisplayNameFields fragment.
 *
 * Returns a Map<string, string> from URN → human-readable display name.
 */
export function useResolveEntityNames(transactions: ChangeTransaction[] | undefined): Map<string, string> {
    const entityRegistry = useEntityRegistry();

    const urns = useMemo(() => {
        if (!transactions) return [];
        const urnSet = new Set<string>();

        transactions.forEach((tx) => {
            (tx.changes ?? []).forEach((change) => {
                if (change?.modifier?.startsWith(URN_PREFIX)) {
                    urnSet.add(change.modifier);
                }
                (change?.parameters ?? []).forEach((param) => {
                    if (param?.value?.startsWith(URN_PREFIX)) {
                        urnSet.add(param.value);
                    }
                });
            });
        });

        return Array.from(urnSet);
    }, [transactions]);

    const { data, error } = useGetEntityDisplayNamesQuery({
        skip: urns.length === 0,
        variables: { urns },
    });

    if (error) {
        console.warn('Failed to resolve entity display names for Change History sidebar', error);
    }

    return useMemo(() => {
        const nameMap = new Map<string, string>();
        if (!data?.entities) return nameMap;

        data.entities
            .filter((entity): entity is NonNullable<typeof entity> => entity != null)
            .forEach((entity) => {
                const name = entityRegistry.getDisplayName(entity.type, entity as Entity);
                if (name && name !== entity.urn) {
                    nameMap.set(entity.urn, name);
                }
            });

        return nameMap;
    }, [data, entityRegistry]);
}
