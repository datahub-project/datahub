import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Maybe, ExecutionRequestResult } from '@src/types.generated';
import { useGetSearchResultsForMultipleQuery } from '../../../graphql/search.generated';
import { EmbeddedListSearchModal } from '../../entity/shared/components/styled/search/EmbeddedListSearchModal';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { UnionType } from '../../search/utils/constants';
import { formatNumber } from '../../shared/formatNumber';
import { Message } from '../../shared/Message';
import { useEntityRegistry } from '../../useEntityRegistry';
import { extractEntityTypeCountsFromFacets, getEntitiesIngestedByType, getTotalEntitiesIngested } from './utils';

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
    max-width: 100%;
    flex-wrap: wrap;
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

type Props = {
    id: string;
    executionResult?: Maybe<Partial<ExecutionRequestResult>>;
};

const ENTITY_FACET_NAME = 'entity';
const TYPE_NAMES_FACET_NAME = 'typeNames';

export default function IngestedAssets({ id, executionResult }: Props) {
    const entityRegistry = useEntityRegistry();

    // First thing to do is to search for all assets with the id as the run id!
    const [showAssetSearch, setShowAssetSearch] = useState(false);

    // Try getting the counts via the ingestion report.
    const totalEntitiesIngested = executionResult && getTotalEntitiesIngested(executionResult);
    const entitiesIngestedByTypeFromReport = executionResult && getEntitiesIngestedByType(executionResult);

    // Fallback to the search across entities.
    // First thing to do is to search for all assets with the id as the run id!
    // Execute search
    const { data, loading, error } = useGetSearchResultsForMultipleQuery({
        skip: totalEntitiesIngested === null || entitiesIngestedByTypeFromReport === null,
        variables: {
            input: {
                query: '*',
                start: 0,
                count: 1,
                filters: [
                    {
                        field: 'runId',
                        values: [id],
                    },
                ],
            },
        },
        fetchPolicy: 'cache-first',
    });

    // Parse filter values to get results.
    const facets = data?.searchAcrossEntities?.facets;

    // Extract facets to construct the per-entity type breakdown stats
    const hasEntityTypeFacet = (facets || []).findIndex((facet) => facet.field === ENTITY_FACET_NAME) >= 0;
    const entityTypeFacets =
        (hasEntityTypeFacet && facets?.filter((facet) => facet.field === ENTITY_FACET_NAME)[0]) || undefined;
    const hasSubTypeFacet = (facets || []).findIndex((facet) => facet.field === TYPE_NAMES_FACET_NAME) >= 0;
    const subTypeFacets =
        (hasSubTypeFacet && facets?.filter((facet) => facet.field === TYPE_NAMES_FACET_NAME)[0]) || undefined;

    const countsByEntityType =
        entitiesIngestedByTypeFromReport ??
        (entityTypeFacets ? extractEntityTypeCountsFromFacets(entityRegistry, entityTypeFacets, subTypeFacets) : []);

    // The total number of assets ingested
    const total = totalEntitiesIngested ?? data?.searchAcrossEntities?.total ?? 0;

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
                    title="View Ingested Assets"
                    searchBarStyle={{ width: 600, marginRight: 40 }}
                    fixedFilters={{
                        unionType: UnionType.AND,
                        filters: [{ field: 'runId', values: [id] }],
                    }}
                    onClose={() => setShowAssetSearch(false)}
                />
            )}
        </>
    );
}
