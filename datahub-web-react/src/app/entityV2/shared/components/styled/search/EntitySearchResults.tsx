import { Checkbox, List } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { EntityPath, EntityType, SearchResult } from '../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../../entity/shared/constants';
import { MATCHES_CONTAINER_HEIGHT } from '../../../../../searchV2/SearchResultList';
import { MatchContextContianer } from '../../../../../searchV2/matches/MatchContextContainer';
import { PreviewSection } from '../../../../../shared/MatchesContext';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityAndType } from '../../../types';

export const StyledList = styled(List)`
    overflow-y: auto;
    height: 100%;
    background-color: ${ANTD_GRAY[3]};
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    flex: 1;
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
    &::-webkit-scrollbar {
        height: 12px;
        width: 5px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
` as typeof List;

const StyledCheckbox = styled(Checkbox)`
    margin-right: 12px;
`;

export const ListItem = styled.div<{ isSelectMode: boolean; areMatchesExpanded }>`
    padding: 20px;
    display: flex;
    align-items: center;
    background-color: #ffffff;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.08);
    margin-bottom: ${(props) => (props.areMatchesExpanded ? MATCHES_CONTAINER_HEIGHT + 20 : 20)}px;
    transition: margin-bottom 0.3s ease;
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
};

const MatchContextAndEntityContainer = styled.div`
    position: relative;
    margin: 16px;
`;

export const EntitySearchResults = ({
    additionalPropertiesList,
    searchResults,
    isSelectMode,
    selectedEntities = [],
    setSelectedEntities,
    bordered = true,
    entityAction,
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

    return (
        <StyledList
            bordered={bordered}
            dataSource={searchResults}
            renderItem={(searchResult) => {
                const { entity } = searchResult;
                const expandedSection = urnToExpandedSection[entity.urn];
                return (
                    <MatchContextAndEntityContainer>
                        <MatchContextContianer
                            selected={false}
                            item={searchResult}
                            onClick={() => {}}
                            urnToExpandedSection={urnToExpandedSection}
                            setUrnToExpandedSection={setUrnToExpandedSection}
                        >
                            <ListItem isSelectMode={isSelectMode || false} areMatchesExpanded={expandedSection}>
                                {isSelectMode && (
                                    <StyledCheckbox
                                        checked={selectedEntityUrns.indexOf(entity.urn) >= 0}
                                        onChange={(e) =>
                                            onSelectEntity({ urn: entity.urn, type: entity.type }, e.target.checked)
                                        }
                                    />
                                )}
                                {entityRegistry.renderSearchResult(entity.type, searchResult)}
                                {entityAction && <EntityAction urn={entity.urn} type={entity.type} />}
                            </ListItem>
                        </MatchContextContianer>
                    </MatchContextAndEntityContainer>
                );
            }}
        />
    );
};
