import { useMemo } from 'react';

import {
    DocumentDomainGroup,
    buildDocumentDomainGroups,
    getDocumentDomainGroupField,
} from '@app/document/utils/documentSidebarGrouping';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useSearchDocumentsQuery } from '@graphql/document.generated';

/** Same cap as Metrics group roots / document filter facet dropdowns. */
const GROUP_FACET_MAX = 100;

/**
 * Domain group headers for the documents sidebar — facet aggregations via
 * searchDocuments (same visibility as the expanded group lists), plus an
 * Unassigned bucket when any visible document has no domain.
 */
export default function useDocumentGroupRoots(
    unassignedLabel: string,
    activeGroup?: DocumentDomainGroup,
    viewUrn?: string | null,
    skip?: boolean,
): { groups: DocumentDomainGroup[]; loading: boolean } {
    const entityRegistry = useEntityRegistry();
    const field = getDocumentDomainGroupField();

    const { data, previousData, loading } = useSearchDocumentsQuery({
        skip,
        fetchPolicy: 'network-only',
        variables: {
            input: {
                query: '*',
                start: 0,
                // Facets only — documents list is unused for group headers.
                count: 0,
                viewUrn: viewUrn ?? undefined,
                // Match Metrics useMetricsGroupRoots: only the domains facet, raised bucket cap.
                facets: [field],
                searchFlags: { maxAggValues: GROUP_FACET_MAX },
            },
        },
    });

    const {
        data: unassignedData,
        previousData: previousUnassignedData,
        loading: unassignedLoading,
    } = useSearchDocumentsQuery({
        skip,
        fetchPolicy: 'network-only',
        variables: {
            input: {
                query: '*',
                start: 0,
                count: 0,
                hasDomain: false,
                viewUrn: viewUrn ?? undefined,
            },
        },
    });

    const groups = useMemo(() => {
        const resolvedData = data ?? previousData;
        const facet = (resolvedData?.searchDocuments?.facets ?? []).find((item) => item?.field === field);
        const resolvedUnassignedData = unassignedData ?? previousUnassignedData;
        const unassignedCount = resolvedUnassignedData?.searchDocuments?.total ?? 0;

        return buildDocumentDomainGroups({
            aggregations: facet?.aggregations ?? [],
            unassignedCount,
            unassignedLabel,
            activeGroup,
            getDisplayName: (entity) => entityRegistry.getDisplayName(entity.type, entity),
        });
    }, [
        activeGroup,
        data,
        entityRegistry,
        field,
        previousData,
        previousUnassignedData,
        unassignedData,
        unassignedLabel,
    ]);

    return {
        groups,
        loading: skip ? false : loading || unassignedLoading,
    };
}
