import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { EmbeddedListSearchModal } from '@app/entity/shared/components/styled/search/EmbeddedListSearchModal';
import {
    extractEntityTypeCountsFromFacets,
    getEntitiesIngestedByTypeOrSubtype,
    getIngestionContents,
    getOtherIngestionContents,
    getTotalEntitiesIngested,
} from '@app/ingestV2/source/utils';
import { UnionType } from '@app/search/utils/constants';
import { Message } from '@app/shared/Message';
import { formatNumber } from '@app/shared/formatNumber';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, Card, Heading, Pill, Text } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { ExecutionRequestResult, Maybe } from '@src/types.generated';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';

// Base flex container with common spacing
const FlexContainer = styled.div`
    display: flex;
    gap: 16px;
`;

const SectionSmallTitle = styled.div`
    padding-top: 16px;
    padding-bottom: 4px;
`;

const SubTitleContainer = styled.div`
    padding-top: 8px;
`;

const MainContainer = styled(FlexContainer)`
    align-items: stretch;
    margin-top: 16px;
`;

const TotalSection = styled.div`
    display: flex;
    flex-direction: column;
    align-items: stretch;
`;

const TypesSection = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: stretch;
    width: 100%;
`;

// the contents of this div are a bunch of cards which should take up the full width of the container
const IngestionBoxesContainer = styled(FlexContainer)`
    flex-direction: row;
    flex-wrap: wrap;
    width: 100%;

    /* Make cards expand to fill available space */
    & > * {
        flex: 1;
        min-width: 200px; /* Ensure cards don't get too narrow */
    }
`;

const EntityCountsContainer = styled(FlexContainer)`
    flex: 1;
    width: 100%;
    align-items: stretch;
    justify-content: flex-start;
    flex-wrap: wrap;

    /* Make cards expand to fill available space */
    & > * {
        flex: 1;
        min-width: 130px; /* Ensure cards don't get too narrow */
    }
`;

const TypesHeaderContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-top: 16px;
    position: relative;
`;

const TypesHeader = styled(Text)``;

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

const IngestionRowCount = styled(Text)`
    margin-right: 10px;
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
            <Card
                title={
                    <IngestionBoxTopRow>
                        <IngestionRowCount size="xl" weight="bold" color="gray" colorLevel={800}>
                            {formatNumber(item.count)}
                        </IngestionRowCount>
                        <Pill
                            size="sm"
                            variant="filled"
                            color="gray"
                            label={item.count === 0 ? 'Missing' : `${item.percent} of Total`}
                        />
                    </IngestionBoxTopRow>
                }
                subTitle={
                    <Text size="md" color="gray" colorLevel={600}>
                        {getLabel(item)}
                    </Text>
                }
                key={getKey(item)}
            />
        ))}
    </IngestionBoxesContainer>
);

export default function IngestedAssets({ id, executionResult }: Props) {
    const entityRegistry = useEntityRegistry();

    // First thing to do is to search for all assets with the id as the run id!
    const [showAssetSearch, setShowAssetSearch] = useState(false);

    // Try getting the counts via the ingestion report.
    const totalEntitiesIngested = executionResult && getTotalEntitiesIngested(executionResult);
    const entitiesIngestedByTypeFromReport = executionResult && getEntitiesIngestedByTypeOrSubtype(executionResult);

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
    console.log('entitiesIngestedByTypeFromReport', entitiesIngestedByTypeFromReport);
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

    console.log('entityTypeFacets', entityTypeFacets);
    console.log('subTypeFacets', subTypeFacets);
    console.log('countsByEntityType', countsByEntityType);
    console.log('facets', facets);

    // The total number of assets ingested
    const total = totalEntitiesIngested ?? data?.searchAcrossEntities?.total ?? 0;

    const ingestionContents = useMemo(() => {
        if (!executionResult) return undefined;
        try {
            return getIngestionContents(executionResult);
        } catch (err) {
            console.error('Error getting ingestion contents:', err);
            return undefined;
        }
    }, [executionResult]);

    const otherIngestionContents = useMemo(() => {
        if (!executionResult) return undefined;
        try {
            return getOtherIngestionContents(executionResult);
        } catch (err) {
            console.error('Error getting other ingestion contents:', err);
            return undefined;
        }
    }, [executionResult]);

    return (
        <>
            {error && <Message type="error" content="" />}
            <Heading type="h4" size="lg" weight="bold">
                Assets
            </Heading>
            <Text color="gray" colorLevel={600}>
                Types and counts for this ingestion run.
            </Text>
            {loading && (
                <Text color="gray" colorLevel={600}>
                    Loading...
                </Text>
            )}
            {!loading && total === 0 && <Text>No assets were ingested.</Text>}
            {!loading && total > 0 && (
                <>
                    <MainContainer>
                        <TotalSection>
                            <Card
                                title={formatNumber(total)}
                                button={
                                    <Button
                                        style={{ width: '110px' }}
                                        variant="text"
                                        onClick={() => setShowAssetSearch(true)}
                                    >
                                        View All
                                    </Button>
                                }
                                subTitle={
                                    <Text size="md" color="gray" colorLevel={600} style={{ marginTop: 2 }}>
                                        Total Assets Ingested
                                    </Text>
                                }
                            />
                        </TotalSection>
                        <VerticalDivider />
                        <TypesSection>
                            <EntityCountsContainer>
                                {countsByEntityType.map((entityCount) => (
                                    <Card
                                        title={formatNumber(entityCount.count)}
                                        subTitle={
                                            <Text size="md" color="gray" colorLevel={600}>
                                                {capitalizeFirstLetterOnly(entityCount.displayName)}
                                            </Text>
                                        }
                                        key={entityCount.displayName}
                                    ></Card>
                                ))}
                            </EntityCountsContainer>
                        </TypesSection>
                    </MainContainer>
                    {ingestionContents && (
                        <IngestionContentsContainer>
                            <Heading type="h5" size="lg" weight="bold">
                                Coverage
                            </Heading>
                            <SubTitleContainer>
                                <Text color="gray" colorLevel={600} lineHeight="sm">
                                    Additional metadata collected during this ingestion run.
                                </Text>
                            </SubTitleContainer>
                            <SectionSmallTitle>
                                <Text weight="bold" size="sm">
                                    Lineage
                                </Text>
                            </SectionSmallTitle>
                            <IngestionContents
                                items={ingestionContents}
                                getKey={(item) => item.title || ''}
                                getLabel={(item) => item.title || ''}
                            />
                            {otherIngestionContents && (
                                <>
                                    <SectionSmallTitle>
                                        <Text weight="bold" size="sm">
                                            Statistics
                                        </Text>
                                    </SectionSmallTitle>
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
