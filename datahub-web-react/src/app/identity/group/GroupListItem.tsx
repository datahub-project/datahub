import { LockOutlined } from '@ant-design/icons';
import { List, Tag, Tooltip, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import EntityDropdown from '@app/entity/shared/EntityDropdown';
import { EntityMenuItems } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { getElasticCappedTotalValueText } from '@app/entity/shared/constants';
import SelectRoleGroup from '@app/identity/group/SelectRoleGroup';
import CustomAvatar from '@app/shared/avatar/CustomAvatar';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpGroup, DataHubRole, EntityType, OriginType } from '@types';

type Props = {
    group: CorpGroup;
    onDelete?: () => void;
    selectRoleOptions: Array<DataHubRole>;
    refetch?: () => void;
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

const GroupItemButtonGroup = styled.div`
    display: flex;
    justify-content: space-evenly;
    align-items: center;
`;

export default function GroupListItem({ group, onDelete, selectRoleOptions, refetch }: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);
    const isExternalGroup: boolean = group.origin?.type === OriginType.External;
    const externalGroupType: string = group.origin?.externalType || 'outside DataHub';
    const castedCorpUser = group as any;
    const groupRelationships = castedCorpUser?.roles?.relationships;
    const userRole =
        groupRelationships && groupRelationships.length > 0 && (groupRelationships[0]?.entity as DataHubRole);
    const groupRoleUrn = userRole && userRole.urn;

    return (
        <List.Item>
            <GroupItemContainer>
                <Link to={`${entityRegistry.getEntityUrl(EntityType.CorpGroup, group.urn)}`}>
                    <GroupHeaderContainer>
                        <CustomAvatar
                            size={32}
                            name={displayName}
                            photoUrl={group?.editableProperties?.pictureLink || undefined}
                        />
                        <div style={{ marginLeft: 16, marginRight: 16 }}>
                            <div>
                                <Typography.Text>{displayName}</Typography.Text>
                            </div>
                            <div>
                                <Typography.Text type="secondary">{group.properties?.description}</Typography.Text>
                            </div>
                        </div>
                        <Tag>{getElasticCappedTotalValueText((group as any).memberCount?.total || 0)} members</Tag>
                    </GroupHeaderContainer>
                </Link>
                <GroupItemButtonGroup>
                    {isExternalGroup && (
                        <Tooltip
                            title={`Membership for this group cannot be edited as it is synced from ${externalGroupType}.`}
                        >
                            <LockOutlined />
                        </Tooltip>
                    )}
                    <SelectRoleGroup
                        group={group}
                        groupRoleUrn={groupRoleUrn || ''}
                        selectRoleOptions={selectRoleOptions}
                        refetch={refetch}
                    />
                    <EntityDropdown
                        urn={group.urn}
                        entityType={EntityType.CorpGroup}
                        entityData={group}
                        menuItems={new Set([EntityMenuItems.DELETE])}
                        size={20}
                        onDeleteEntity={onDelete}
                        options={{ hideDeleteMessage: false, skipDeleteWait: true }}
                    />
                </GroupItemButtonGroup>
            </GroupItemContainer>
        </List.Item>
    );
}
