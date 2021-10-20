import React from 'react';
import { Divider, List } from 'antd';
import styled from 'styled-components';
import { Entity } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { IconStyleType } from '../../../entity/Entity';

const StyledList = styled(List)`
    margin-top: -1px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
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

const ListItem = styled.div`
    padding-right: 40px;
    padding-left: 40px;
    padding-top: 16px;
    padding-bottom: 8px;
`;

const ThinDivider = styled(Divider)`
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
            bordered
            dataSource={entities}
            renderItem={(entity, index) => {
                const genericProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
                const platformLogoUrl = genericProps?.platform?.info?.logoUrl;
                const platformName = genericProps?.platform?.info?.displayName;
                const entityTypeName = entityRegistry.getEntityName(entity.type);
                const displayName = entityRegistry.getDisplayName(entity.type, entity);
                const url = entityRegistry.getEntityUrl(entity.type, entity.urn);
                const fallbackIcon = entityRegistry.getIcon(entity.type, 18, IconStyleType.ACCENT);
                return (
                    <>
                        <ListItem>
                            <DefaultPreviewCard
                                name={displayName}
                                logoUrl={platformLogoUrl || undefined}
                                logoComponent={fallbackIcon}
                                url={url}
                                platform={platformName || undefined}
                                type={entityTypeName}
                                titleSizePx={14}
                                onClick={() => onClick?.(index)}
                            />
                        </ListItem>
                        <ThinDivider />
                    </>
                );
            }}
        />
    );
};
