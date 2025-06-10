import { Text, Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { OwnerLabel } from '@app/shared/OwnerLabel';
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

const MAX_OWNERS_TO_SHOW = 2;

export const renderOwners = (owners: Owner[], entityRegistry: EntityRegistry) => {
    if (!owners) {
        return <div>No owners</div>;
    }
    const truncatedOwners = owners.slice(0, MAX_OWNERS_TO_SHOW);
    return (
        <div>
            {truncatedOwners.map((owner) => {
                const ownerEntity = owner.owner;
                const name = entityRegistry.getDisplayName(ownerEntity.type, ownerEntity);
                const avatarUrl =
                    ownerEntity.type === EntityType.CorpUser
                        ? ownerEntity.editableProperties?.pictureLink || undefined
                        : undefined;
                return <OwnerLabel name={name} avatarUrl={avatarUrl} type={ownerEntity.type} />;
            })}
            {owners.length > MAX_OWNERS_TO_SHOW && (
                <Tooltip
                    title={
                        <div>
                            {owners.slice(MAX_OWNERS_TO_SHOW).map((owner) => (
                                <Text>{entityRegistry.getDisplayName(owner.owner.type, owner.owner)}</Text>
                            ))}
                        </div>
                    }
                >
                    <Text style={{ display: 'inline-block' }}>+{owners.length - MAX_OWNERS_TO_SHOW} more</Text>
                </Tooltip>
            )}
        </div>
    );
};
