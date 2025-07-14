import { Button, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EmbeddedListSearchModal } from '@app/entity/shared/components/styled/search/EmbeddedListSearchModal';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import {
    extractEntityTypeCountsFromFacets,
    getEntitiesIngestedByType,
    getIngestionContents,
    getTotalEntitiesIngested,
} from '@app/ingestV2/source/utils';
import { UnionType } from '@app/search/utils/constants';
import { Message } from '@app/shared/Message';
import { formatNumber } from '@app/shared/formatNumber';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { ExecutionRequestResult, Maybe } from '@src/types.generated';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';

const MainContainer = styled.div`
    display: flex;
    align-items: stretch;
    gap: 16px;
    margin-top: 16px;
`;

const TotalContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
    padding: 16px 20px;
    background-color: ${ANTD_GRAY[1]};
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 4px;
    min-width: 200px;
    min-height: 80px;
    gap: 16px;
`;

const TotalInfo = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
`;

const TotalText = styled(Typography.Text)`
    font-size: 18px;
    color: ${ANTD_GRAY[8]};
    font-weight: bold;
`;

const TotalLabel = styled(Typography.Text)`
    font-size: 12px;
    color: ${ANTD_GRAY[6]};
    margin-top: 4px;
`;

const TypesSection = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    position: relative;
    padding-top: 20px;
`;

const EntityCountsContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: stretch;
    max-width: 100%;
    flex-wrap: wrap;
    gap: 16px;
`;

const EntityCountsHeader = styled(Typography.Text)`
    font-size: 12px;
    color: ${ANTD_GRAY[6]};
    position: absolute;
    top: 0;
    left: 0;
    margin-bottom: 0;
    z-index: 1;
`;

const EntityCount = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    background-color: ${ANTD_GRAY[1]};
    border: 1px solid ${ANTD_GRAY[4]};
    padding: 16px 20px;
    border-radius: 4px;
    min-width: 70px;
    min-height: 80px;
`;

const ViewAllButton = styled(Button)`
    padding: 0px;
    margin-top: 4px;
`;

const VerticalDivider = styled.div`
    width: 1px;
    background-color: ${ANTD_GRAY[4]};
    height: 70px;
    align-self: center;
`;

const IngestionContentsContainer = styled.div`
    margin-top: 40px;
`;

const IngestionBoxesContainer = styled.div`
    display: flex;
    gap: 16px;
    width: 100%;
`;

const IngestionBox = styled.div<{ boxCount: number }>`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    background: ${ANTD_GRAY[1]};
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 4px;
    min-width: 0;
    min-height: 80px;
    padding: 16px 20px;
    flex: 1 1 0;
    max-width: ${({ boxCount }) => `calc(${100 / boxCount}% - ${(16 * (boxCount - 1)) / boxCount}px)`};
`;

const IngestionBoxTopRow = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: center;
    width: 100%;
`;

const IngestionBoxPercent = styled(Typography.Text)`
    margin-left: 8px;
    font-size: 12px;
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
        skip: typeof totalEntitiesIngested === 'number' && !!entitiesIngestedByTypeFromReport?.length,
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

    const casingForTypes = (input: string) => {
        return input.charAt(0).toUpperCase() + input.slice(1);
    };

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

    const ingestionContents = executionResult && getIngestionContents(executionResult);

    return (
        <>
            {error && <Message type="error" content="" />}
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
            {!loading && total > 0 && (
                <>
                    <MainContainer>
                        <TotalContainer>
                            <TotalInfo>
                                <TotalText>{formatNumber(total)}</TotalText>
                                <TotalLabel>Total Assets Ingested</TotalLabel>
                            </TotalInfo>
                            <ViewAllButton type="link" onClick={() => setShowAssetSearch(true)}>
                                View All
                            </ViewAllButton>
                        </TotalContainer>
                        <VerticalDivider />
                        <TypesSection>
                            <EntityCountsHeader>Types</EntityCountsHeader>
                            <EntityCountsContainer>
                                {countsByEntityType.map((entityCount) => (
                                    <EntityCount key={entityCount.displayName}>
                                        <Typography.Text
                                            style={{ fontSize: 16, color: ANTD_GRAY[8], fontWeight: 'bold' }}
                                        >
                                            {formatNumber(entityCount.count)}
                                        </Typography.Text>
                                        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                                            {casingForTypes(entityCount.displayName)}
                                        </Typography.Text>
                                    </EntityCount>
                                ))}
                            </EntityCountsContainer>
                        </TypesSection>
                    </MainContainer>
                    {ingestionContents && (
                        <IngestionContentsContainer>
                            <Typography.Title level={5}>Ingestion Contents</Typography.Title>
                            <Typography.Paragraph type="secondary">
                                Breakdown of assets containing recommended ingestion data.
                            </Typography.Paragraph>
                            <IngestionBoxesContainer>
                                {ingestionContents.map((item) => (
                                    <IngestionBox key={item.title} boxCount={ingestionContents.length}>
                                        <IngestionBoxTopRow>
                                            <Typography.Text
                                                style={{ fontSize: 16, color: ANTD_GRAY[8], fontWeight: 'bold' }}
                                            >
                                                {formatNumber(item.count)}
                                            </Typography.Text>
                                            <IngestionBoxPercent type="secondary">
                                                {item.percent} of Total
                                            </IngestionBoxPercent>
                                        </IngestionBoxTopRow>
                                        <Typography.Text type="secondary" style={{ fontSize: 12, marginTop: 4 }}>
                                            {item.title}
                                        </Typography.Text>
                                    </IngestionBox>
                                ))}
                            </IngestionBoxesContainer>
                        </IngestionContentsContainer>
                    )}
                </>
            )}
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
