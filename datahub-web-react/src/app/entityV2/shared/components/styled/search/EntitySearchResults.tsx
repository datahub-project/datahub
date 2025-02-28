import { Checkbox, Empty, List } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { EntityPath, EntityType, SearchResult } from '../../../../../../types.generated';
import { EntityAndType } from '../../../../../entity/shared/types';
import { useSearchContext } from '../../../../../search/context/SearchContext';
import { MATCHES_CONTAINER_HEIGHT } from '../../../../../searchV2/SearchResultList';
import { MatchContextContainer } from '../../../../../searchV2/matches/MatchContextContainer';
import { PreviewSection } from '../../../../../shared/MatchesContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { useInitializeColumnLineageCards } from './useInitializeColumnLineageCards';

export const StyledList = styled(List)`
    height: 100%;
    flex: 1;
    overflow: auto;

    .ant-list-items > .ant-list-item {
        padding-right: 0px;
        padding-left: 0px;
    }

    > .ant-list-header {
        padding-right: 0px;
        padding-left: 0px;
        font-size: 14px;
        font-weight: 600;
        margin-left: -20px;
        border-bottom: none;
        padding-bottom: 0px;
        padding-top: 15px;
    }
` as typeof List;

const StyledCheckbox = styled(Checkbox)`
    margin-right: 12px;
`;

export const ListItem = styled.div<{ isSelectMode: boolean; areMatchesExpanded; compactUserSearchCardStyle: boolean }>`
    padding: 20px;
    display: flex;
    align-items: center;
    background-color: #ffffff;
    border-radius: 10px;
    overflow: hidden;
    margin-bottom: ${({ areMatchesExpanded, compactUserSearchCardStyle }) => {
        if (!areMatchesExpanded) return 0;
        if (compactUserSearchCardStyle) return MATCHES_CONTAINER_HEIGHT;
        return MATCHES_CONTAINER_HEIGHT + 20;
    }}px;
    transition: margin-bottom 0.3s ease;
    border: 1px solid #ebecf0;
    ${(props) =>
        props.areMatchesExpanded &&
        `
        -webkit-box-shadow: 0px 0px 24px 0px rgba(0, 0, 0, 0.15);
        -moz-box-shadow: 0px 0px 24px 0px rgba(0, 0, 0, 0.15);
         box-shadow: 0px 0px 24px 0px rgba(0, 0, 0, 0.15);
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
    selectedEntities?: EntityAndType[];
    setSelectedEntities?: (entities: EntityAndType[]) => any;
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
    const onSelectEntity = (selectedEntity: EntityAndType, selected: boolean) => {
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
        <StyledList
            locale={{
                emptyText: (
                    <>
                        <Empty
                            description={noResultsMessage || 'No results found'}
                            image={Empty.PRESENTED_IMAGE_SIMPLE}
                        />
                    </>
                ),
            }}
            bordered={bordered}
            dataSource={searchResults}
            renderItem={(searchResult) => {
                const { entity } = searchResult;
                const expandedSection = isFullViewCard ? urnToExpandedSection[entity.urn] : undefined;
                return (
                    <MatchContextAndEntityContainer>
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
                                    <StyledCheckbox
                                        checked={selectedEntityUrns.indexOf(entity.urn) >= 0}
                                        disabled={
                                            selectLimit !== undefined &&
                                            selectedEntities.length >= selectLimit &&
                                            !selectedEntityUrns.includes(entity.urn)
                                        }
                                        onChange={(e) =>
                                            onSelectEntity({ urn: entity.urn, type: entity.type }, e.target.checked)
                                        }
                                    />
                                )}
                                {entityRegistry.renderSearchResult(entity.type, searchResult)}
                                {entityAction && <EntityAction urn={entity.urn} type={entity.type} />}
                            </ListItem>
                        </MatchContextContainer>
                    </MatchContextAndEntityContainer>
                );
            }}
        />
    );
};
