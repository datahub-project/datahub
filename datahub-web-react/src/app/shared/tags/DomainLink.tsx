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

const DomainWrapper = styled.span`
    display: inline-block;
    margin-bottom: 8px;
`;

interface DomainContentProps {
    domain: Domain;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
}

function DomainContent({ domain, name, closable, onClose, tagStyle }: DomainContentProps) {
    const entityRegistry = useEntityRegistry();

    const displayName = name || entityRegistry.getDisplayName(EntityType.Domain, domain);

    return (
        <Tag style={tagStyle} closable={closable} onClose={onClose}>
            <span style={{ paddingRight: '4px' }}>
                {entityRegistry.getIcon(EntityType.Domain, 10, IconStyleType.ACCENT)}
            </span>
            {displayName}
        </Tag>
    );
}

export type Props = {
    domain: Domain;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
    readOnly?: boolean;
};

export const DomainLink = ({ domain, name, closable, onClose, tagStyle, readOnly }: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const urn = domain?.urn;

    if (readOnly) {
        return (
            <HoverEntityTooltip entity={domain}>
                <DomainWrapper>
                    <DomainContent
                        domain={domain}
                        name={name}
                        closable={closable}
                        onClose={onClose}
                        tagStyle={tagStyle}
                    />
                </DomainWrapper>
            </HoverEntityTooltip>
        );
    }

    return (
        <HoverEntityTooltip entity={domain}>
            <DomainLinkContainer to={entityRegistry.getEntityUrl(EntityType.Domain, urn)}>
                <DomainContent domain={domain} name={name} closable={closable} onClose={onClose} tagStyle={tagStyle} />
            </DomainLinkContainer>
        </HoverEntityTooltip>
    );
};
