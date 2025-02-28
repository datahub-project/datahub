import React from 'react';
import { Checkbox, Radio } from 'antd';
import styled from 'styled-components';
import { EntityPath, EntityType, SearchResult } from '../../../../../../types.generated';
import { EntityAndType } from '../../../types';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { ListItem, StyledList, ThinDivider } from '../../../../../recommendations/renderer/component/EntityNameList';

const StyledCheckbox = styled(Checkbox)`
    margin-right: 12px;
`;
const StyledRadio = styled(Radio)`
    margin-right: 12px;
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
    singleSelect?: boolean;
    entityAction?: React.FC<EntityActionProps>;
};

export const EntitySearchResults = ({
    additionalPropertiesList,
    searchResults,
    isSelectMode,
    selectedEntities = [],
    setSelectedEntities,
    bordered = true,
    singleSelect,
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
        if (singleSelect && selected) {
            setSelectedEntities?.([selectedEntity]);
        } else if (selected) {
            setSelectedEntities?.([...selectedEntities, selectedEntity]);
        } else {
            setSelectedEntities?.(selectedEntities?.filter((entity) => entity.urn !== selectedEntity.urn) || []);
        }
    };

    const EntityAction = entityAction as React.FC<EntityActionProps>;

    return (
        <StyledList
            bordered={bordered}
            dataSource={searchResults}
            renderItem={(searchResult) => {
                const { entity } = searchResult;
                return (
                    <>
                        <ListItem isSelectMode={isSelectMode || false}>
                            {singleSelect
                                ? isSelectMode && (
                                      <StyledRadio
                                          className="radioButton"
                                          checked={selectedEntityUrns.indexOf(entity.urn) >= 0}
                                          onChange={(e) =>
                                              onSelectEntity(
                                                  {
                                                      urn: entity.urn,
                                                      type: entity.type,
                                                  },
                                                  e.target.checked,
                                              )
                                          }
                                      />
                                  )
                                : isSelectMode && (
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
                        <ThinDivider />
                    </>
                );
            }}
        />
    );
};
