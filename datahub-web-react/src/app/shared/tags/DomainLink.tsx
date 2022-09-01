import { Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Domain, EntityType } from '../../../types.generated';
import { IconStyleType } from '../../entity/Entity';
import { HoverEntityTooltip } from '../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../useEntityRegistry';

const DomainLinkContainer = styled(Link)`
    display: inline-block;
    margin-bottom: 8px;
`;

export type Props = {
    domain: Domain;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
};

export const DomainLink = ({ domain, name, closable, onClose, tagStyle }: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const urn = domain?.urn;
    const displayName = name || entityRegistry.getDisplayName(EntityType.Domain, domain);

    return (
        <HoverEntityTooltip entity={domain}>
            <DomainLinkContainer to={entityRegistry.getEntityUrl(EntityType.Domain, urn)}>
                <Tag style={tagStyle} closable={closable} onClose={onClose}>
                    <span style={{ paddingRight: '4px' }}>
                        {entityRegistry.getIcon(EntityType.Domain, 10, IconStyleType.ACCENT)}
                    </span>
                    {displayName}
                </Tag>
            </DomainLinkContainer>
        </HoverEntityTooltip>
    );
};
