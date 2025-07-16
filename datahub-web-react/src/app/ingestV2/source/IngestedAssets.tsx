import { Button } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { EmbeddedListSearchModal } from '@app/entity/shared/components/styled/search/EmbeddedListSearchModal';
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
import { Heading, Pill, Text } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { ExecutionRequestResult, Maybe } from '@src/types.generated';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';

// Base flex container with common spacing
const FlexContainer = styled.div`
    display: flex;
    gap: 16px;
`;

// Base card styling
const BaseCard = styled.div`
    display: flex;
    padding: 12px;
    background-color: white;
    border: 1px solid ${colors.gray[1400]};
    border-radius: 12px;
    box-shadow: 0px 4px 8px 0px rgba(33, 23, 95, 0.04);
    min-height: 60px;
`;

const MainContainer = styled(FlexContainer)`
    align-items: stretch;
    margin-top: 16px;
`;

const CardContainer = styled(BaseCard)`
    flex-direction: column;
    justify-content: center;
    align-items: flex-start;
    flex: 1 0 0;
    min-height: 80px;
    max-height: 80px;
`;

const TotalContainer = styled(BaseCard)`
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
    flex: 1 0 0;
    min-height: 80px;
    max-height: 80px;
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
    width: 100%;
`;

const IngestionBoxesContainer = styled(FlexContainer)`
    width: 100%;
`;

const EntityCountsContainer = styled(FlexContainer)`
    flex: 1;
    width: 100%;
    align-items: stretch;
    justify-content: flex-start;
    flex-wrap: wrap;
`;

const TypesHeaderContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-top: 16px;
    position: relative;
`;

const TypesHeader = styled(Text)`
    position: absolute;
    top: 0;
    left: calc(50% + 33px);
    margin-bottom: 0;
    z-index: 1;
`;

const VerticalDivider = styled.div`
    width: 2px;
    background-color: ${colors.gray[1400]};
    height: 80px;
    align-self: center;
`;

const IngestionContentsContainer = styled.div`
    margin-top: 20px;
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

const IngestionContents: React.FC<RenderIngestionContentsProps> = ({ items, getKey, getLabel }) => (
    <IngestionBoxesContainer>
        {items.map((item) => (
            <CardContainer key={getKey(item)}>
                <IngestionBoxTopRow>
                    <Text size="xl" weight="bold" color="gray" colorLevel={800} style={{ marginRight: 10 }}>
                        {formatNumber(item.count)}
                    </Text>
                    <Pill
                        size="sm"
                        variant="version"
                        color="white"
                        label={item.count === 0 ? 'Missing' : `${item.percent} of Total`}
                    />
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

    const ingestionContents = useMemo(
        () => executionResult && getIngestionContents(executionResult),
        [executionResult],
    );
    const otherIngestionContents = useMemo(
        () => executionResult && getOtherIngestionContents(executionResult),
        [executionResult],
    );

    return (
        <>
            {error && <Message type="error" content="" />}
            <Heading type="h4" size="lg" weight="bold">
                Assets
            </Heading>
            {loading && (
                <Text color="gray" colorLevel={600}>
                    Loading...
                </Text>
            )}
            {!loading && total === 0 && <Text>No assets were ingested.</Text>}
            {!loading && total > 0 && (
                <>
                    <TypesHeaderContainer>
                        <Text color="gray" colorLevel={600}>
                            Types and counts for this ingestion run.
                        </Text>
                        <TypesHeader size="sm" color="gray" colorLevel={600} weight="bold">
                            Types
                        </TypesHeader>
                    </TypesHeaderContainer>
                    <MainContainer>
                        <TotalContainer>
                            <TotalInfo>
                                <Text size="xl" weight="bold" color="gray" colorLevel={800}>
                                    {formatNumber(total)}
                                </Text>
                                <Text size="md" color="gray" colorLevel={600} style={{ marginTop: 2 }}>
                                    Total Assets Ingested
                                </Text>
                            </TotalInfo>
                            <Button type="link" onClick={() => setShowAssetSearch(true)}>
                                View All
                            </Button>
                        </TotalContainer>
                        <VerticalDivider />
                        <TypesSection>
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
                            <Heading type="h5" size="lg" weight="bold">
                                Coverage
                            </Heading>
                            <Text color="gray" colorLevel={600}>
                                Additional metadata collected during this ingestion run.
                            </Text>
                            <Text weight="semiBold" size="md">
                                Lineage
                            </Text>
                            <IngestionContents
                                items={ingestionContents}
                                getKey={(item) => item.title || ''}
                                getLabel={(item) => item.title || ''}
                            />
                            {otherIngestionContents && (
                                <>
                                    <Text weight="semiBold" size="md">
                                        Statistics
                                    </Text>
                                    <IngestionContents
                                        items={otherIngestionContents}
                                        getKey={(item) => item.type || ''}
                                        getLabel={(item) => item.type || ''}
                                    />
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
