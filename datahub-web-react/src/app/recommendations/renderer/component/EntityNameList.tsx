import React from 'react';
import { Divider, List } from 'antd';
import styled from 'styled-components';
import { Entity } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { IconStyleType } from '../../../entity/Entity';
import { getPlatformName } from '../../../entity/shared/utils';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';

export const StyledList = styled(List)`
    overflow-y: auto;
    height: 100%;
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

export const ListItem = styled.div<{ isSelectMode: boolean }>`
    padding-right: 40px;
    padding-left: ${(props) => (props.isSelectMode ? '20px' : '40px')};
    padding-top: 16px;
    padding-bottom: 8px;
    display: flex;
    align-items: center;
`;

export const ThinDivider = styled(Divider)`
    padding: 0px;
    margin: 0px;
`;

type Props = {
    entities: Array<Entity>;
    onClick?: (index: number) => void;
};

export const EntityNameList = ({ entities, onClick }: Props) => {
    const entityRegistry = useEntityRegistry();

    return (
        <StyledList
            dataSource={entities}
            renderItem={(entity, index) => {
                const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
                const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
                const platformName = getPlatformName(genericProps);
                const entityTypeName = entityRegistry.getEntityName(entity.type);
                const displayName = entityRegistry.getDisplayName(entity.type, entity);
                const url = entityRegistry.getEntityUrl(entity.type, entity.urn);
                const fallbackIcon = entityRegistry.getIcon(entity.type, 18, IconStyleType.ACCENT);
                const subType = capitalizeFirstLetterOnly(genericProps?.subTypes?.typeNames?.[0]);
                const entityCount = genericProps?.entityCount;
                const deprecation = genericProps?.deprecation;
                const health = genericProps?.health;

                return (
                    <>
                        <ListItem isSelectMode={false}>
                            <DefaultPreviewCard
                                name={displayName}
                                urn={entity.urn}
                                logoUrl={platformLogoUrl || undefined}
                                logoComponent={fallbackIcon}
                                url={url}
                                platform={platformName}
                                type={subType || entityTypeName}
                                titleSizePx={14}
                                tags={genericProps?.globalTags || undefined}
                                glossaryTerms={genericProps?.glossaryTerms || undefined}
                                domain={genericProps?.domain?.domain}
                                onClick={() => onClick?.(index)}
                                entityCount={entityCount}
                                deprecation={deprecation}
                                health={health || undefined}
                            />
                        </ListItem>
                        <ThinDivider />
                    </>
                );
            }}
        />
    );
};
