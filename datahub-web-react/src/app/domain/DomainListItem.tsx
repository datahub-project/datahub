import React from 'react';
import styled from 'styled-components';
import { List, Tag, Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { IconStyleType } from '../entity/Entity';
import { Domain, EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import AvatarsGroup from '../shared/avatar/AvatarsGroup';
import EntityDropdown from '../entity/shared/EntityDropdown';
import { EntityMenuItems } from '../entity/shared/EntityDropdown/EntityDropdown';

const DomainItemContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-left: 8px;
    padding-right: 8px;
    width: 100%;
`;

const DomainHeaderContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const DomainNameContainer = styled.div`
    margin-left: 16px;
    margin-right: 16px;
`;

type Props = {
    domain: Domain;
    onDelete?: () => void;
};

export default function DomainListItem({ domain, onDelete }: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.Domain, domain);
    const logoIcon = entityRegistry.getIcon(EntityType.Domain, 12, IconStyleType.ACCENT);
    const owners = domain.ownership?.owners;
    const totalEntities = domain.entities?.total;

    return (
        <List.Item>
            <DomainItemContainer>
                <Link to={entityRegistry.getEntityUrl(EntityType.Domain, domain.urn)}>
                    <DomainHeaderContainer>
                        {logoIcon}
                        <DomainNameContainer>
                            <Typography.Text>{displayName}</Typography.Text>
                        </DomainNameContainer>
                        <Tooltip title={`There are ${totalEntities} entities in this domain.`}>
                            <Tag>{totalEntities || 0} entities</Tag>
                        </Tooltip>
                    </DomainHeaderContainer>
                </Link>
                {owners && owners.length > 0 && (
                    <AvatarsGroup size={24} owners={owners} entityRegistry={entityRegistry} maxCount={4} />
                )}
                <EntityDropdown
                    urn={domain.urn}
                    entityType={EntityType.Domain}
                    entityData={domain}
                    menuItems={new Set([EntityMenuItems.DELETE])}
                    size={20}
                    onDeleteEntity={onDelete}
                />
            </DomainItemContainer>
        </List.Item>
    );
}
