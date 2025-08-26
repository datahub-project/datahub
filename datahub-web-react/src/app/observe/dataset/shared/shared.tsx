import { Text, Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { AvatarStack } from '@components/components/AvatarStack/AvatarStack';
import { AvatarType } from '@components/components/AvatarStack/types';

import { colors } from '@src/alchemy-components';
import { EntityRegistry } from '@src/entityRegistryContext';

import { EntityType, Owner } from '@types';

export const Header = styled.div`
    && {
        padding-left: 2px;
        padding-right: 2px;
        padding-bottom: 8px;
        padding-top: 8px;
    }
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

export const Title = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    margin-bottom: 12px;
`;

export const TitleText = styled(Typography.Title)`
    && {
        margin: 0px;
        padding: 0px;
        color: ${colors.gray[600]};
    }
`;

export const DescriptionText = styled.span`
    font-size: 16px;
    color: ${colors.gray[1700]};
`;

export const Stat = styled.div`
    display: flex;
    align-items: end;
    justify-content: left;
    margin-bottom: 10px;
`;

export const Total = styled.span`
    font-size: 52px;
    line-height: 48px;
    margin-right: 4px;
    color: ${colors.gray[600]};
`;

export const Percent = styled.span`
    font-size: 14px;
    color: ${colors.gray[1700]};
    padding-bottom: 2px;
`;

export const List = styled.div`
    border-color: ${colors.gray[100]};
    border-width: 1px;
`;

const MAX_OWNERS_TO_SHOW = 3;

export const renderOwners = (owners: Owner[], entityRegistry: EntityRegistry) => {
    if (!owners.length) {
        return null;
    }
    return (
        <div>
            <Tooltip
                title={
                    <div>
                        {owners.map((owner) => (
                            <Text>{entityRegistry.getDisplayName(owner.owner.type, owner.owner)}</Text>
                        ))}
                    </div>
                }
            >
                <div>
                    <AvatarStack
                        avatars={owners.map((owner) => ({
                            name: entityRegistry.getDisplayName(owner.owner.type, owner.owner),
                            avatarUrl:
                                owner.owner.type === EntityType.CorpUser
                                    ? owner.owner.editableProperties?.pictureLink || undefined
                                    : undefined,
                            type: owner.owner.type === EntityType.CorpUser ? AvatarType.user : AvatarType.group,
                        }))}
                        size="md"
                        maxToShow={MAX_OWNERS_TO_SHOW}
                    />
                </div>
            </Tooltip>
        </div>
    );
};
