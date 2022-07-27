import React from 'react';
import { Divider, List, Checkbox } from 'antd';
import styled from 'styled-components';
import { Entity } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { IconStyleType } from '../../../entity/Entity';
import { capitalizeFirstLetter } from '../../../shared/textUtil';
import { EntityAndType } from '../../../entity/shared/types';

const StyledCheckbox = styled(Checkbox)`
    margin-right: 12px;
`;

const StyledList = styled(List)`
    overflow-y: scroll;
    height: 100%;
    margin-top: -1px;
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

const ListItem = styled.div<{ isSelectMode: boolean }>`
    padding-right: 40px;
    padding-left: ${(props) => (props.isSelectMode ? '20px' : '40px')};
    padding-top: 16px;
    padding-bottom: 8px;
    display: flex;
    align-items: center;
`;

const ThinDivider = styled(Divider)`
    padding: 0px;
    margin: 0px;
`;

type AdditionalProperties = {
    degree?: number;
};

type Props = {
    // additional data about the search result that is not part of the entity used to enrich the
    // presentation of the entity. For example, metadata about how the entity is related for the case
    // of impact analysis
    additionalPropertiesList?: Array<AdditionalProperties>;
    entities: Array<Entity>;
    onClick?: (index: number) => void;
    isSelectMode?: boolean;
    selectedEntities?: EntityAndType[];
    setSelectedEntities?: (entities: EntityAndType[]) => any;
    bordered?: boolean;
};

export const EntityNameList = ({
    additionalPropertiesList,
    entities,
    onClick,
    isSelectMode,
    selectedEntities = [],
    setSelectedEntities,
    bordered = true,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const selectedEntityUrns = selectedEntities?.map((entity) => entity.urn) || [];

    if (
        additionalPropertiesList?.length !== undefined &&
        additionalPropertiesList.length > 0 &&
        additionalPropertiesList?.length !== entities.length
    ) {
        console.warn(
            'Warning: additionalPropertiesList length provided to EntityNameList does not match entity array length',
            { additionalPropertiesList, entities },
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

    return (
        <StyledList
            bordered={bordered}
            dataSource={entities}
            renderItem={(entity, index) => {
                const additionalProperties = additionalPropertiesList?.[index];
                const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
                const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
                const platformName =
                    genericProps?.platform?.properties?.displayName ||
                    capitalizeFirstLetter(genericProps?.platform?.name);
                const entityTypeName = entityRegistry.getEntityName(entity.type);
                const displayName = entityRegistry.getDisplayName(entity.type, entity);
                const url = entityRegistry.getEntityUrl(entity.type, entity.urn);
                const fallbackIcon = entityRegistry.getIcon(entity.type, 18, IconStyleType.ACCENT);
                const subType = genericProps?.subTypes?.typeNames?.length && genericProps?.subTypes?.typeNames[0];
                const entityCount = genericProps?.entityCount;
                return (
                    <>
                        <ListItem isSelectMode={isSelectMode || false}>
                            {isSelectMode && (
                                <StyledCheckbox
                                    checked={selectedEntityUrns.indexOf(entity.urn) >= 0}
                                    onChange={(e) =>
                                        onSelectEntity({ urn: entity.urn, type: entity.type }, e.target.checked)
                                    }
                                />
                            )}
                            <DefaultPreviewCard
                                name={displayName}
                                logoUrl={platformLogoUrl || undefined}
                                logoComponent={fallbackIcon}
                                url={url}
                                platform={platformName || undefined}
                                type={subType || entityTypeName}
                                titleSizePx={14}
                                tags={genericProps?.globalTags || undefined}
                                glossaryTerms={genericProps?.glossaryTerms || undefined}
                                domain={genericProps?.domain?.domain}
                                onClick={() => onClick?.(index)}
                                entityCount={entityCount}
                                degree={additionalProperties?.degree}
                            />
                        </ListItem>
                        <ThinDivider />
                    </>
                );
            }}
        />
    );
};
