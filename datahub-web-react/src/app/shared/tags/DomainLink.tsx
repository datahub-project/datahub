import { Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { EntityType } from '../../../types.generated';
import { IconStyleType } from '../../entity/Entity';
import { useEntityRegistry } from '../../useEntityRegistry';

const DomainLinkContainer = styled(Link)`
    display: inline-block;
    margin-bottom: 8px;
`;

export type Props = {
    urn: string;
    name: string;
    closable?: boolean;
    onClose?: (e: any) => void;
};

export const DomainLink = ({ urn, name, closable, onClose }: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    return (
        <DomainLinkContainer to={entityRegistry.getEntityUrl(EntityType.Domain, urn)}>
            <Tag closable={closable} onClose={onClose}>
                <span style={{ paddingRight: '4px' }}>
                    {entityRegistry.getIcon(EntityType.Domain, 10, IconStyleType.ACCENT)}
                </span>
                {name}
            </Tag>
        </DomainLinkContainer>
    );
};
