import React from 'react';
import styled from 'styled-components';
import { List, Tag, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { IconStyleType } from '../entity/Entity';
import { Domain, EntityType } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';

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
};

export default function DomainListItem({ domain }: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.Domain, domain);
    const logoIcon = entityRegistry.getIcon(EntityType.Domain, 12, IconStyleType.ACCENT);

    return (
        <List.Item>
            <DomainItemContainer>
                <Link to={entityRegistry.getEntityUrl(EntityType.Domain, domain.urn)}>
                    <DomainHeaderContainer>
                        {logoIcon}
                        <DomainNameContainer>
                            <Typography.Text>{displayName}</Typography.Text>
                        </DomainNameContainer>
                        <Tag>{(domain as any).entities?.total || 0} entities</Tag>
                    </DomainHeaderContainer>
                </Link>
            </DomainItemContainer>
        </List.Item>
    );
}
