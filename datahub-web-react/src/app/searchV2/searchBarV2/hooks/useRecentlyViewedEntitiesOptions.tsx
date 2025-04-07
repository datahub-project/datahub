import React, { useMemo } from 'react';
import SectionHeader from '../components/SectionHeader';
import useRecentlyViewedEntities from './useRecentlyViewedEntities';
import AutoCompleteEntityItem from '../../autoCompleteV2/AutoCompleteEntityItem';

export default function useRecentlyViewedEntitiesOptions() {
    const { entities: recentlyViewedEntities } = useRecentlyViewedEntities();

    const recentlyViewedEntitiesOptions = useMemo(
        () => ({
            label: <SectionHeader text="You Recently Viewed" />,
            options: recentlyViewedEntities.map((entity) => ({
                label: <AutoCompleteEntityItem entity={entity} />,
                value: entity.urn,
                type: entity.type,
                style: { padding: '0 8px' },
            })),
        }),
        [recentlyViewedEntities],
    );

    return recentlyViewedEntitiesOptions;
}
