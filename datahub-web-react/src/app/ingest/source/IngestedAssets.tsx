import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import { useGetSearchResultsForMultipleQuery } from '../../../graphql/search.generated';
import { EntityType, FacetMetadata } from '../../../types.generated';
import { EmbeddedListSearchModal } from '../../entity/shared/components/styled/search/EmbeddedListSearchModal';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import { Message } from '../../shared/Message';
import { capitalizeFirstLetterOnly, pluralize } from '../../shared/textUtil';

const ENTITIES_WITH_SUBTYPES = new Set([
    EntityType.Dataset.toLowerCase(),
    EntityType.Container.toLowerCase(),
    EntityType.Notebook.toLowerCase(),
]);

type Props = {
    urn: string;
    id: string;
};

type EntityTypeCount = {
    count: number;
    displayName: string;
};

/**
 * Extract entity type counts to display in the ingestion summary.
 *
 * @param entityTypeFacets the filter facets for entity type.
 * @param subTypeFacets the filter facets for sub types.
 */
const extractEntityTypeCountsFromFacets = (
    entityTypeFacets: FacetMetadata,
    subTypeFacets?: FacetMetadata | null,
): EntityTypeCount[] => {
    const finalCounts: EntityTypeCount[] = [];

    if (subTypeFacets) {
        subTypeFacets.aggregations.forEach((agg) =>
            finalCounts.push({
                count: agg.count,
                displayName: pluralize(agg.count, capitalizeFirstLetterOnly(agg.value) || ''),
            }),
        );
        entityTypeFacets.aggregations
            .filter((agg) => !ENTITIES_WITH_SUBTYPES.has(agg.value.toLowerCase()))
            .forEach((agg) =>
                finalCounts.push({
                    count: agg.count,
                    displayName: pluralize(agg.count, capitalizeFirstLetterOnly(agg.value) || ''),
                }),
            );
    } else {
        // Only use Entity Types- no subtypes.
        entityTypeFacets.aggregations.forEach((agg) =>
            finalCounts.push({
                count: agg.count,
                displayName: pluralize(agg.count, capitalizeFirstLetterOnly(agg.value) || ''),
            }),
        );
    }

    return finalCounts;
};

export default function IngestedAssets({ urn, id }: Props) {
    // First thing to do is to search for all assets with the id as the run id!
    console.log(urn);

    const [showAssetSearch, setShowAssetSearch] = useState(false);

    // Execute search
    const { data, loading, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                start: 0,
                count: 1,
                filters: [
                    {
                        field: 'runId',
                        value: id,
                    },
                ],
            },
        },
    });

    // Parse filter values to get results.
    const facets = data?.searchAcrossEntities?.facets;
    const entityTypeFacets = facets?.filter((facet) => facet.field === 'entity')[0];

    const hasSubTypeFacet = (facets?.findIndex((facet) => facet.field === 'typeNames') || -1) >= 0;
    const subTypeFacets = (hasSubTypeFacet && facets?.filter((facet) => facet.field === 'typeNames')[0]) || undefined;
    const countsByEntityType =
        (entityTypeFacets && extractEntityTypeCountsFromFacets(entityTypeFacets, subTypeFacets)) || [];
    const total = data?.searchAcrossEntities?.total || 0;

    return (
        <>
            {error && <Message type="error" content="" />}
            <>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <div>
                        <Typography.Title level={5}>Ingested Assets</Typography.Title>
                        {(loading && <Typography.Text type="secondary">Loading...</Typography.Text>) || (
                            <>
                                {(total > 0 && (
                                    <Typography.Paragraph type="secondary">
                                        The following asset types were ingested during this run.
                                    </Typography.Paragraph>
                                )) || (
                                    <Typography.Text style={{ fontSize: 12 }}>No assets were ingested.</Typography.Text>
                                )}
                            </>
                        )}
                    </div>
                    {!loading && (
                        <div
                            style={{
                                display: 'flex',
                                flexDirection: 'column',
                                justifyContent: 'right',
                                alignItems: 'end',
                            }}
                        >
                            <Typography.Text type="secondary">Total</Typography.Text>
                            <Typography.Text style={{ fontSize: 16, color: ANTD_GRAY[8] }}>
                                <b>{formatNumber(total)}</b> assets
                            </Typography.Text>
                        </div>
                    )}
                </div>
                <div
                    style={{
                        display: 'flex',
                        justifyContent: 'left',
                        alignItems: 'center',
                    }}
                >
                    {countsByEntityType.map((entityCount) => (
                        <span style={{ marginRight: 40 }}>
                            <div
                                style={{
                                    display: 'flex',
                                    flexDirection: 'column',
                                    alignItems: 'flex-start',
                                }}
                            >
                                <Typography.Text style={{ paddingLeft: 2, fontSize: 18, color: ANTD_GRAY[8] }}>
                                    <b>{formatNumber(entityCount.count)}</b>
                                </Typography.Text>
                                <Typography.Text type="secondary">{entityCount.displayName}</Typography.Text>
                            </div>
                        </span>
                    ))}
                </div>
                <div style={{ marginTop: 4 }}>
                    <Button style={{ padding: 0 }} type="link" onClick={() => setShowAssetSearch(true)}>
                        View All
                    </Button>
                </div>
            </>
            {showAssetSearch && (
                <EmbeddedListSearchModal
                    searchBarStyle={{ width: 600, marginRight: 40 }}
                    fixedFilter={{ field: 'runId', value: id }}
                    onClose={() => setShowAssetSearch(false)}
                />
            )}
        </>
    );
}
