import { useEffect } from 'react';
import { useGetEntityDisplayNameLazyQuery } from '../../../graphql/search.generated';
import { getLastBrowseEntryFromFilterValue } from './utils';
import { BROWSE_PATH_V2_FILTER_NAME } from '../utils/constants';
import EntityRegistry from '../../entity/EntityRegistry';

export default function useGetBrowseV2LabelOverride(
    filterField: string,
    filterValue: string,
    entityRegistry: EntityRegistry,
) {
    const [getEntityDisplayName, { data, loading }] = useGetEntityDisplayNameLazyQuery();

    useEffect(() => {
        if (filterField === BROWSE_PATH_V2_FILTER_NAME) {
            const lastBrowseEntry = getLastBrowseEntryFromFilterValue(filterValue);
            if (lastBrowseEntry.includes('urn:li:')) {
                getEntityDisplayName({ variables: { urn: lastBrowseEntry } });
            }
        }
    }, [filterField, filterValue, getEntityDisplayName]);

    if (loading) {
        return '...';
    }

    return data?.entity && entityRegistry.getDisplayName(data.entity.type, data.entity);
}
