import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { useGetSearchResultsForMultipleQuery } from '../../../graphql/search.generated';
import { EntityType, FacetMetadata } from '../../../types.generated';
import { EmbeddedListSearchModal } from '../../entity/shared/components/styled/search/EmbeddedListSearchModal';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { formatNumber } from '../../shared/formatNumber';
import { Message } from '../../shared/Message';
import { capitalizeFirstLetterOnly, pluralize } from '../../shared/textUtil';

const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const TitleContainer = styled.div``;

const TotalContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: right;
    align-items: end;
`;

const TotalText = styled(Typography.Text)`
    font-size: 16px;
    color: ${ANTD_GRAY[8]};
`;

const EntityCountsContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const EntityCount = styled.div`
    margin-right: 40px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
`;

const ViewAllButton = styled(Button)`
    padding: 0px;
    margin-top: 4px;
`;

const ENTITIES_WITH_SUBTYPES = new Set([
    EntityType.Dataset.toLowerCase(),
    EntityType.Container.toLowerCase(),
    EntityType.Notebook.toLowerCase(),
]);

type Props = {
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

export default function IngestedAssets({ id }: Props) {
    // First thing to do is to search for all assets with the id as the run id!
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
            <HeaderContainer>
                <TitleContainer>
                    <Typography.Title level={5}>Ingested Assets</Typography.Title>
                    {(loading && <Typography.Text type="secondary">Loading...</Typography.Text>) || (
                        <>
                            {(total > 0 && (
                                <Typography.Paragraph type="secondary">
                                    The following asset types were ingested during this run.
                                </Typography.Paragraph>
                            )) || <Typography.Text>No assets were ingested.</Typography.Text>}
                        </>
                    )}
                </TitleContainer>
                {!loading && (
                    <TotalContainer>
                        <Typography.Text type="secondary">Total</Typography.Text>
                        <TotalText style={{ fontSize: 16, color: ANTD_GRAY[8] }}>
                            <b>{formatNumber(total)}</b> assets
                        </TotalText>
                    </TotalContainer>
                )}
            </HeaderContainer>
            <EntityCountsContainer>
                {countsByEntityType.map((entityCount) => (
                    <EntityCount>
                        <Typography.Text style={{ paddingLeft: 2, fontSize: 18, color: ANTD_GRAY[8] }}>
                            <b>{formatNumber(entityCount.count)}</b>
                        </Typography.Text>
                        <Typography.Text type="secondary">{entityCount.displayName}</Typography.Text>
                    </EntityCount>
                ))}
            </EntityCountsContainer>
            <ViewAllButton type="link" onClick={() => setShowAssetSearch(true)}>
                View All
            </ViewAllButton>
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
