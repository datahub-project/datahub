import { Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EmbeddedListSearchModal } from '@app/entity/shared/components/styled/search/EmbeddedListSearchModal';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import {
    extractEntityTypeCountsFromFacets,
    getEntitiesIngestedByType,
    getIngestionContents,
    getOtherIngestionContents,
    getTotalEntitiesIngested,
} from '@app/ingestV2/source/utils';
import { UnionType } from '@app/search/utils/constants';
import { Message } from '@app/shared/Message';
import { formatNumber } from '@app/shared/formatNumber';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Heading, Text } from '@src/alchemy-components';
import { ExecutionRequestResult, Maybe } from '@src/types.generated';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';

const MainContainer = styled.div`
    display: flex;
    align-items: stretch;
    gap: 16px;
    margin-top: 16px;
`;

const CardContainer = styled.div`
    display: flex;
    padding: 16px;
    flex-direction: column;
    justify-content: center;
    align-items: flex-start;
    gap: 16px;
    flex: 1 0 0;
    background-color: ${ANTD_GRAY[1]};
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 12px;
    min-height: 80px;
`;

const TotalContainer = styled(CardContainer)`
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
`;

const TotalInfo = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
`;

const TypesSection = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: stretch;
    position: relative;
    padding-top: 20px;
    width: 100%;
`;

const EntityCountsContainer = styled.div`
    display: flex;
    justify-content: flex-start;
    align-items: stretch;
    width: 100%;
    flex-wrap: wrap;
    gap: 16px;
    flex: 1;
`;

const EntityCountsHeader = styled(Text)`
    position: absolute;
    top: 0;
    left: 0;
    margin-bottom: 0;
`;

const VerticalDivider = styled.div`
    width: 2px;
    background-color: ${ANTD_GRAY[4]};
    height: 120px;
    align-self: center;
`;

const IngestionContentsContainer = styled.div`
    margin-top: 10px;
`;

const IngestionBoxesContainer = styled.div`
    display: flex;
    gap: 16px;
    width: 100%;
`;

const IngestionBoxTopRow = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: flex-start;
    width: 100%;
`;

type Props = {
    id: string;
    executionResult?: Maybe<Partial<ExecutionRequestResult>>;
};

const ENTITY_FACET_NAME = 'entity';
const TYPE_NAMES_FACET_NAME = 'typeNames';

type IngestionContentItem = {
    title?: string;
    type?: string;
    count: number;
    percent: string;
};

type RenderIngestionContentsProps = {
    items: IngestionContentItem[];
    getKey: (item: IngestionContentItem) => string;
    getLabel: (item: IngestionContentItem) => string;
};

const renderIngestionContents = ({ items, getKey, getLabel }: RenderIngestionContentsProps) => (
    <IngestionBoxesContainer>
        {items.map((item) => (
            <CardContainer key={getKey(item)}>
                <IngestionBoxTopRow>
                    <Text size="xl" weight="bold" color="gray" colorLevel={800}>
                        {formatNumber(item.count)}
                    </Text>
                    <Text size="sm" color="gray" colorLevel={600} style={{ marginLeft: 8 }}>
                        {item.percent} of Total
                    </Text>
                </IngestionBoxTopRow>
                <Text size="md" color="gray" colorLevel={600}>
                    {getLabel(item)}
                </Text>
            </CardContainer>
        ))}
    </IngestionBoxesContainer>
);

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
    const otherIngestionContents = executionResult && getOtherIngestionContents(executionResult);

    return (
        <>
            {error && <Message type="error" content="" />}
            <Heading type="h5" size="lg" weight="medium">
                Ingested Assets
            </Heading>
            {(loading && (
                <Text color="gray" colorLevel={600}>
                    Loading...
                </Text>
            )) || (
                <>
                    {(total > 0 && (
                        <Text color="gray" colorLevel={600}>
                            The following asset types were ingested during this run.
                        </Text>
                    )) || <Text>No assets were ingested.</Text>}
                </>
            )}
            {!loading && total > 0 && (
                <>
                    <MainContainer>
                        <TotalContainer>
                            <TotalInfo>
                                <Text size="xl" weight="bold" color="gray" colorLevel={800}>
                                    {formatNumber(total)}
                                </Text>
                                <Text size="md" color="gray" colorLevel={600} style={{ marginTop: 4 }}>
                                    Total Assets Ingested
                                </Text>
                            </TotalInfo>
                            <Button type="link" onClick={() => setShowAssetSearch(true)}>
                                View All
                            </Button>
                        </TotalContainer>
                        <VerticalDivider />
                        <TypesSection>
                            <EntityCountsHeader size="xs" color="gray" colorLevel={600}>
                                Types
                            </EntityCountsHeader>
                            <EntityCountsContainer>
                                {countsByEntityType.map((entityCount) => (
                                    <CardContainer key={entityCount.displayName}>
                                        <Text size="xl" weight="bold" color="gray" colorLevel={800}>
                                            {formatNumber(entityCount.count)}
                                        </Text>
                                        <Text size="md" color="gray" colorLevel={600}>
                                            {capitalizeFirstLetterOnly(entityCount.displayName)}
                                        </Text>
                                    </CardContainer>
                                ))}
                            </EntityCountsContainer>
                        </TypesSection>
                    </MainContainer>
                    {ingestionContents && (
                        <IngestionContentsContainer>
                            <Heading type="h5" size="lg" weight="medium">
                                Ingestion Contents
                            </Heading>
                            <Text color="gray" colorLevel={600}>
                                Breakdown of assets containing recommended ingestion data.
                            </Text>
                            <Text
                                weight="medium"
                                size="sm"
                                style={{
                                    marginTop: 16,
                                    marginBottom: 8,
                                    display: 'block',
                                }}
                            >
                                Lineage Types
                            </Text>
                            {renderIngestionContents({
                                items: ingestionContents,
                                getKey: (item) => item.title || '',
                                getLabel: (item) => item.title || '',
                            })}
                            {otherIngestionContents && (
                                <>
                                    <Text
                                        weight="medium"
                                        size="sm"
                                        style={{
                                            marginTop: 24,
                                            marginBottom: 8,
                                            display: 'block',
                                        }}
                                    >
                                        Other Ingestion Contents
                                    </Text>
                                    {renderIngestionContents({
                                        items: otherIngestionContents,
                                        getKey: (item) => item.type || '',
                                        getLabel: (item) => item.type || '',
                                    })}
                                </>
                            )}
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
