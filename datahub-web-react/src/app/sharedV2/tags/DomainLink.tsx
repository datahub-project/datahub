import { CloseOutlined } from '@ant-design/icons';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Domain as DomainEntity, EntityType } from '@types';

const DomainLinkContainer = styled(Link)`
    display: inline-block;
`;

const DomainWrapper = styled.span`
    display: inline-block;
    margin-bottom: 8px;
`;

const CloseButton = styled.div`
    margin-left: 4px;
    :hover {
        cursor: pointer;
    }
    && {
        color: ${ANTD_GRAY[7]};
    }
`;

const StyledCloseOutlined = styled(CloseOutlined)`
    && {
        font-size: 10px;
    }
`;

const StyledTag = styled.div<{ fontSize?: number }>`
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
    display: flex;
    align-items: center;
    justify-content: start;
    gap: 5px;
`;

interface DomainContentProps {
    domain: DomainEntity;
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
        <StyledTag style={tagStyle} fontSize={fontSize}>
            <DomainColoredIcon domain={domain} size={28} />
            {displayName}
            {closable && (
                <CloseButton onClick={onClose}>
                    <StyledCloseOutlined />
                </CloseButton>
            )}
        </StyledTag>
    );
}

export type Props = {
    domain: DomainEntity;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
    readOnly?: boolean;
    fontSize?: number;
};

export const DomainLink = ({ domain, name, closable, onClose, tagStyle, readOnly, fontSize }: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const linkProps = useEmbeddedProfileLinkProps();
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
            <DomainLinkContainer to={entityRegistry.getEntityUrl(EntityType.Domain, urn)} {...linkProps}>
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
