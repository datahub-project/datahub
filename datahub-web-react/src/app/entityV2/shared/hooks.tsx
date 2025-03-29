import { useEffect, useState } from 'react';
import { useLocation } from 'react-router';

/**
 * Hook for managing the expanded row keys based on the `assertion_urn | incident_urn` query parameter.
 *
 * This hook ensures that relevant rows are expanded if an `assertion_urn | incident_urn` is present in the URL query parameters.
 * If no `assertion_urn | incident_urn` is provided, it expands all groups initially.
 *
 * @param {Array} groups - Array of assertion groups, where each group contains a list of assertions.
 * @returns {Object} - Object containing the expanded row keys and a function to update them.
 *  - expandedRowKeys: Array of currently expanded row keys.
 *  - setExpandedRowKeys: Function to manually set the expanded row keys.
 */
export const useGetExpandedTableGroupsFromEntityUrnInUrl = (
    groups,
    { isGroupBy }: { isGroupBy: boolean },
    entityUrnQueryParameter: string,
    getGroupEntities: (group) => { urn: string }[],
) => {
    const location = useLocation();
    const resourceUrnParam = new URLSearchParams(location.search).get(entityUrnQueryParameter);
    const [expandedGroupIds, setExpandedGroupIds] = useState<string[]>([]);
    const [processed, setProcessed] = useState(false);

    useEffect(() => {
        if (isGroupBy) {
            if (resourceUrnParam) {
                const decodedResourceUrn = decodeURIComponent(resourceUrnParam);

                // Find the row key to expand based on the incident URN
                const rowKeyToExpand = groups.find((group) =>
                    getGroupEntities(group).some((item) => item.urn === decodedResourceUrn),
                )?.name;

                if (rowKeyToExpand) {
                    setExpandedGroupIds((prevKeys) => [...prevKeys, rowKeyToExpand]);
                }

                setProcessed(true);
            } else if (!processed) {
                // If no assertion URN is present initially, set expandedGroupIds from groups
                const allGroupKeys = groups.map((group) => group.name);
                setExpandedGroupIds(allGroupKeys);
                setProcessed(true);
            }
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [groups, resourceUrnParam, processed, isGroupBy]);

    return { expandedGroupIds, setExpandedGroupIds };
};
