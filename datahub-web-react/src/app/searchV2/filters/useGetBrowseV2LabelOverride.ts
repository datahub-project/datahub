/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
