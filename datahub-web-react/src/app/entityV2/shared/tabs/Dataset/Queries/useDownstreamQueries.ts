import { useGetDefaultLineageStartTimeMillis } from '@app/lineage/utils/useGetLineageTimeParams';
import { useEffect, useState } from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { useSearchAcrossLineageForQueriesQuery } from '../../../../../../graphql/query.generated';
import { Entity, EntityType, LineageDirection, QueryEntity } from '../../../../../../types.generated';
import { useBaseEntity } from '../../../../../entity/shared/EntityContext';
import { DBT_URN } from '../../../../../ingest/source/builder/constants';
import { LINEAGE_FILTER_PAGINATION, isQuery } from '../../../../../lineageV2/common';
import { DEGREE_FILTER_NAME } from '../../../../../search/utils/constants';
import { MAX_QUERIES_COUNT } from './utils/constants';
import { filterQueries } from './utils/filterQueries';
import { mapQuery } from './utils/mapQuery';

export default function useDownstreamQueries(filterText: string) {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const startTimeMillis = useGetDefaultLineageStartTimeMillis();

    const { data: lineageData, loading } = useSearchAcrossLineageForQueriesQuery({
        variables: {
            input: {
                urn: baseEntity?.dataset?.urn,
                count: MAX_QUERIES_COUNT,
                direction: LineageDirection.Downstream,
                orFilters: [
                    {
                        and: [
                            {
                                field: DEGREE_FILTER_NAME,
                                values: ['1'],
                            },
                        ],
                    },
                ],
                lineageFlags: {
                    startTimeMillis,
                    entitiesExploredPerHopLimit: LINEAGE_FILTER_PAGINATION,
                    ignoreAsHops: [
                        {
                            entityType: EntityType.Dataset,
                            platforms: [DBT_URN],
                        },
                        { entityType: EntityType.DataJob },
                    ],
                },
            },
        },
        skip: !baseEntity?.dataset?.urn,
        fetchPolicy: 'cache-first',
    });

    const [seenQueryUrns] = useState<Set<string>>(new Set());
    const [downstreamQueryEntities, setDownstreamQueryEntities] = useState<[QueryEntity, Entity][]>([]);

    useEffect(() => {
        lineageData?.searchAcrossLineage?.searchResults?.forEach((result) => {
            result.paths?.forEach((path) => {
                if (!path) return;
                const parent = path.path[path.path.length - 2];
                if (!parent) return;
                if (isQuery(parent) && !seenQueryUrns.has(parent.urn)) {
                    setDownstreamQueryEntities((prevQueries) => [
                        ...prevQueries,
                        [parent as QueryEntity, result.entity as Entity],
                    ]);
                    seenQueryUrns.add(parent.urn);
                }
            });
        });
    }, [lineageData, downstreamQueryEntities, seenQueryUrns]);

    const downstreamQueries = filterQueries(
        filterText,
        downstreamQueryEntities.map(([queryEntity, poweredEntity]) =>
            mapQuery({ queryEntity, entityUrn: baseEntity?.dataset?.urn, poweredEntity }),
        ),
    );

    return { downstreamQueries, loading };
}
