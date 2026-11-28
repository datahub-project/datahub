import { Checkbox, EmptyState } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useInitializeColumnLineageCards } from '@app/entityV2/shared/components/styled/search/useInitializeColumnLineageCards';
import { useSearchContext } from '@app/search/context/SearchContext';
import { MATCHES_CONTAINER_HEIGHT } from '@app/searchV2/SearchResultList';
import { MatchContextContainer } from '@app/searchV2/matches/MatchContextContainer';
import { PreviewSection } from '@app/sharedV2/MatchesContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityPath, EntityType, SearchResult } from '@types';

const ResultsList = styled.div<{ $bordered: boolean }>`
    height: 100%;
    flex: 1;
    overflow: auto;
    ${({ $bordered, theme }) => $bordered && `border: 1px solid ${theme.colors.border}; border-radius: 8px;`}
`;

const EmptyContainer = styled.div`
    padding: 48px 16px;
`;

const CheckboxWrapper = styled.div`
    margin-right: 12px;
`;

const ListItem = styled.div<{ isSelectMode: boolean; areMatchesExpanded; compactUserSearchCardStyle: boolean }>`
    padding: 20px;
    display: flex;
    align-items: center;
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 10px;
    overflow: hidden;
    margin-bottom: ${({ areMatchesExpanded, compactUserSearchCardStyle }) => {
        if (!areMatchesExpanded) return 0;
        if (compactUserSearchCardStyle) return MATCHES_CONTAINER_HEIGHT;
        return MATCHES_CONTAINER_HEIGHT + 20;
    }}px;
    transition: margin-bottom 0.3s ease;
    border: 1px solid ${(props) => props.theme.colors.border};
    ${(props) =>
        props.areMatchesExpanded &&
        `
        -webkit-box-shadow: ${props.theme.colors.shadowLg};
        -moz-box-shadow: ${props.theme.colors.shadowLg};
         box-shadow: ${props.theme.colors.shadowLg};
    `}
`;

export type EntityActionProps = {
    urn: string;
    type: EntityType;
};

type AdditionalProperties = {
    degree?: number;
    paths?: EntityPath[];
};

type Props = {
    // additional data about the search result that is not part of the entity used to enrich the
    // presentation of the entity. For example, metadata about how the entity is related for the case
    // of impact analysis
    additionalPropertiesList?: Array<AdditionalProperties>;
    searchResults: Array<SearchResult>;
    isSelectMode?: boolean;
    selectedEntities?: Entity[];
    setSelectedEntities?: (entities: Entity[]) => any;
    bordered?: boolean;
    entityAction?: React.FC<EntityActionProps>;
    compactUserSearchCardStyle?: boolean;
    noResultsMessage?: string;
    selectLimit?: number;
};

const MatchContextAndEntityContainer = styled.div`
    position: relative;
    margin: 16px;
    overflow: hidden;
`;

export const EntitySearchResults = ({
    additionalPropertiesList,
    searchResults,
    isSelectMode,
    selectedEntities = [],
    setSelectedEntities,
    bordered = true,
    entityAction,
    compactUserSearchCardStyle,
    noResultsMessage,
    selectLimit,
}: Props) => {
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const selectedEntityUrns = selectedEntities?.map((entity) => entity.urn) || [];

    if (
        additionalPropertiesList?.length !== undefined &&
        additionalPropertiesList.length > 0 &&
        additionalPropertiesList?.length !== searchResults.length
    ) {
        console.warn(
            'Warning: additionalPropertiesList length provided to EntityNameList does not match entity array length',
            { additionalPropertiesList, searchResults },
        );
    }

    /**
     * Invoked when a new entity is selected. Simply updates the state of the list of selected entities.
     */
    const onSelectEntity = (selectedEntity: Entity, selected: boolean) => {
        if (selected) {
            setSelectedEntities?.([...selectedEntities, selectedEntity]);
        } else {
            setSelectedEntities?.(selectedEntities?.filter((entity) => entity.urn !== selectedEntity.urn) || []);
        }
    };

    const EntityAction = entityAction as React.FC<EntityActionProps>;

    const [urnToExpandedSection, setUrnToExpandedSection] = React.useState<Record<string, PreviewSection>>({});

    // when in CLL impact analysis, default open up the column paths card slideout
    useInitializeColumnLineageCards(searchResults, setUrnToExpandedSection);

    const { isFullViewCard } = useSearchContext();

    return (
        <ResultsList $bordered={bordered} data-testid="entity-search-results">
            {searchResults.length === 0 && (
                <EmptyContainer>
                    <EmptyState icon={MagnifyingGlass} title={noResultsMessage || tc('noResults')} size="sm" />
                </EmptyContainer>
            )}
            {searchResults.map((searchResult) => {
                const { entity } = searchResult;
                const expandedSection = isFullViewCard ? urnToExpandedSection[entity.urn] : undefined;
                const isSelected = selectedEntityUrns.includes(entity.urn);
                return (
                    <MatchContextAndEntityContainer key={entity.urn} data-testid={`search-result-row-${entity.urn}`}>
                        <MatchContextContainer
                            selected={false}
                            item={searchResult}
                            onClick={() => {}}
                            urnToExpandedSection={urnToExpandedSection}
                            setUrnToExpandedSection={setUrnToExpandedSection}
                            compactUserSearchCardStyle={compactUserSearchCardStyle}
                        >
                            <ListItem
                                isSelectMode={isSelectMode || false}
                                areMatchesExpanded={expandedSection}
                                compactUserSearchCardStyle={compactUserSearchCardStyle || false}
                            >
                                {isSelectMode && (
                                    <CheckboxWrapper>
                                        <Checkbox
                                            isChecked={isSelected}
                                            isDisabled={
                                                selectLimit !== undefined &&
                                                selectedEntities.length >= selectLimit &&
                                                !isSelected
                                            }
                                            setIsChecked={(checked) => onSelectEntity(entity, checked)}
                                            dataTestId={`checkbox-${entity.urn}`}
                                            aria-label={entityRegistry.getDisplayName(entity.type, entity)}
                                        />
                                    </CheckboxWrapper>
                                )}
                                {entityRegistry.renderSearchResult(entity.type, searchResult)}
                                {entityAction && <EntityAction urn={entity.urn} type={entity.type} />}
                            </ListItem>
                        </MatchContextContainer>
                    </MatchContextAndEntityContainer>
                );
            })}
        </ResultsList>
    );
};
