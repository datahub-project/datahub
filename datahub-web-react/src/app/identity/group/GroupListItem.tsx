import React from 'react';
import styled from 'styled-components';
import { List, Tag, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { CorpGroup, EntityType } from '../../../types.generated';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import EntityDropdown from '../../entity/shared/EntityDropdown';
import { EntityMenuItems } from '../../entity/shared/EntityDropdown/EntityDropdown';

type Props = {
    group: CorpGroup;
    onDelete?: () => void;
};

const GroupItemContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-left: 8px;
    padding-right: 8px;
    width: 100%;
`;

const GroupHeaderContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

export default function GroupListItem({ group, onDelete }: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);
    return (
        <List.Item>
            <GroupItemContainer>
                <Link to={`${entityRegistry.getEntityUrl(EntityType.CorpGroup, group.urn)}`}>
                    <GroupHeaderContainer>
                        <CustomAvatar size={32} name={displayName} />
                        <div style={{ marginLeft: 16, marginRight: 16 }}>
                            <div>
                                <Typography.Text>{displayName}</Typography.Text>
                            </div>
                            <div>
                                <Typography.Text type="secondary">{group.properties?.description}</Typography.Text>
                            </div>
                        </div>
                        <Tag>{(group as any).memberCount?.total || 0} members</Tag>
                    </GroupHeaderContainer>
                </Link>
                <EntityDropdown
                    urn={group.urn}
                    entityType={EntityType.CorpGroup}
                    entityData={group}
                    menuItems={new Set([EntityMenuItems.DELETE])}
                    size={20}
                    onDeleteEntity={onDelete}
                />
            </GroupItemContainer>
        </List.Item>
    );
}
