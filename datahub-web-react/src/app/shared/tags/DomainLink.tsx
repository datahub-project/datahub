import { Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { Domain, EntityType } from '../../../types.generated';
import { HoverEntityTooltip } from '../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';
import DomainIcon from '../../domain/DomainIcon';

const DomainLinkContainer = styled(Link)`
    display: inline-block;
    margin-bottom: 8px;
`;

const DomainWrapper = styled.span`
    display: inline-block;
    margin-bottom: 8px;
`;

const StyledTag = styled(Tag)<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
`;

interface DomainContentProps {
    domain: Domain;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
    fontSize?: number;
}

function DomainContent({ domain, name, closable, onClose, tagStyle, fontSize }: DomainContentProps) {
    const entityRegistry = useEntityRegistry();

    const displayName = name || entityRegistry.getDisplayName(EntityType.Domain, domain);

    return (
        <StyledTag style={tagStyle} closable={closable} onClose={onClose} fontSize={fontSize}>
            <span style={{ paddingRight: '4px' }}>
                <DomainIcon
                    style={{
                        fontSize: 10,
                        color: ANTD_GRAY[9],
                    }}
                />
            </span>
            {displayName}
        </StyledTag>
    );
}

export type Props = {
    domain: Domain;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
    readOnly?: boolean;
    fontSize?: number;
};

export const DomainLink = ({ domain, name, closable, onClose, tagStyle, readOnly, fontSize }: Props): JSX.Element => {
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
                        fontSize={fontSize}
                    />
                </DomainWrapper>
            </HoverEntityTooltip>
        );
    }

    return (
        <HoverEntityTooltip entity={domain}>
            <DomainLinkContainer to={entityRegistry.getEntityUrl(EntityType.Domain, urn)}>
                <DomainContent
                    domain={domain}
                    name={name}
                    closable={closable}
                    onClose={onClose}
                    tagStyle={tagStyle}
                    fontSize={fontSize}
                />
            </DomainLinkContainer>
        </HoverEntityTooltip>
    );
};
