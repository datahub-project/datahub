import { useEffect } from 'react';

import { getLastBrowseEntryFromFilterValue } from '@app/searchV2/filters/utils';
import { BROWSE_PATH_V2_FILTER_NAME } from '@app/searchV2/utils/constants';
import { EntityRegistry } from '@src/entityRegistryContext';

import { useGetEntityDisplayNameLazyQuery } from '@graphql/search.generated';

function isEntityUrn(string: string) {
    return string.includes('urn:li:');
}

export default function useGetBrowseV2LabelOverride(
    filterField: string,
    filterValue: string,
    entityRegistry: EntityRegistry,
) {
    const [getEntityDisplayName, { data, loading }] = useGetEntityDisplayNameLazyQuery();

    useEffect(() => {
        if (filterField === BROWSE_PATH_V2_FILTER_NAME) {
            const lastBrowseEntry = getLastBrowseEntryFromFilterValue(filterValue);
            if (isEntityUrn(lastBrowseEntry)) {
                getEntityDisplayName({ variables: { urn: lastBrowseEntry } });
            }
        }
    }, [filterField, filterValue, getEntityDisplayName]);

    if (loading) {
        return '...';
    }

    return data?.entity && entityRegistry.getDisplayName(data.entity.type, data.entity);
}
