import React from 'react';
import useBrowseV2Query from './useBrowseV2Query';
import { AggregationMetadata, EntityType } from '../../../types.generated';
import useIntersect from '../../shared/useIntersect';

type Props = {
    entityAggregation: AggregationMetadata;
    environmentAggregation: AggregationMetadata | null;
    platformAggregation: AggregationMetadata;
    path: Array<string>;
    skip: boolean;
};

const useBrowsePaginator = ({ entityAggregation, environmentAggregation, platformAggregation, path, skip }: Props) => {
    const { loaded, error, groups, pathResult, fetchNextPage } = useBrowseV2Query({
        entityType: entityAggregation.value as EntityType,
        environment: environmentAggregation?.value,
        platform: platformAggregation.value,
        path,
        skip,
    });

    const { observableRef } = useIntersect({
        skip,
        initialDelay: 500,
        options: { rootMargin: '250px' },
        onIntersect: fetchNextPage,
    });

    return {
        loaded,
        error,
        groups,
        pathResult,
        observable: <div ref={observableRef} style={{ width: '1px', height: '1px' }} />,
    } as const;
};

export default useBrowsePaginator;
