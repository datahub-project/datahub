import { Text } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { FontSizeOptions } from '@components/theme/config';

import { DeprecationIcon } from '@app/entityV2/shared/components/styled/DeprecationIcon';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';
import PillRemoveIcon from '@app/sharedV2/icons/PillRemoveIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Domain as DomainEntity, EntityType, MetadataAttribution } from '@types';

const DomainLinkContainer = styled(Link)`
    display: inline-block;
    color: ${(props) => props.theme.colors.text};
    text-decoration: none;

    &:hover,
    &:focus,
    &:active {
        color: ${(props) => props.theme.colors.text};
        text-decoration: none;
    }
`;

const DomainWrapper = styled.span`
    display: inline-block;
`;

const RemoveIcon = styled(PillRemoveIcon)`
    margin-left: 4px;
`;

const PillDeprecationSlot = styled.span`
    display: inline-flex;
    align-items: center;
    & svg {
        width: 12px;
        height: 12px;
    }
`;

const StyledTag = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    gap: 4px;
`;

function domainLinkTextSize(fontSize?: number): FontSizeOptions {
    if (!fontSize || fontSize >= 14) return 'md';
    if (fontSize >= 12) return 'sm';
    return 'xs';
}

interface DomainContentProps {
    domain: DomainEntity;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
    fontSize?: number;
    iconSize?: number;
    iconFontSize?: number;
}

function DomainContent({
    domain,
    name,
    closable,
    onClose,
    tagStyle,
    fontSize,
    iconSize,
    iconFontSize,
}: DomainContentProps) {
    const entityRegistry = useEntityRegistry();
    const displayName = name || entityRegistry.getDisplayName(EntityType.Domain, domain);

    return (
        <StyledTag style={tagStyle} data-testid={`domain-${displayName}`}>
            <DomainColoredIcon domain={domain} size={iconSize || 24} fontSize={iconFontSize ?? 16} />
            <Text type="span" size={domainLinkTextSize(fontSize)} weight="normal" color="inherit">
                {displayName}
            </Text>
            {domain.deprecation && domain.deprecation.deprecated && (
                <PillDeprecationSlot>
                    <DeprecationIcon
                        urn={domain.urn}
                        deprecation={domain.deprecation}
                        showUndeprecate={false}
                        showText={false}
                    />
                </PillDeprecationSlot>
            )}
            {closable && <RemoveIcon onClick={onClose} />}
        </StyledTag>
    );
}

type Props = {
    domain: DomainEntity;
    name?: string;
    closable?: boolean;
    onClose?: (e: any) => void;
    tagStyle?: any | undefined;
    readOnly?: boolean;
    fontSize?: number;
    iconSize?: number;
    iconFontSize?: number;
    enableTooltip?: boolean;
    attribution?: MetadataAttribution | null;
};

export const DomainLink = ({
    domain,
    name,
    closable,
    onClose,
    tagStyle,
    readOnly,
    fontSize,
    iconSize,
    iconFontSize,
    enableTooltip = true,
    attribution,
}: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const linkProps = useEmbeddedProfileLinkProps();
    const urn = domain?.urn;
    const previewContext = attribution ? { propagationDetails: { attribution } } : undefined;

    if (readOnly) {
        return (
            <HoverEntityTooltip entity={domain} canOpen={enableTooltip} previewContext={previewContext}>
                <DomainWrapper>
                    <DomainContent
                        domain={domain}
                        name={name}
                        closable={closable}
                        onClose={onClose}
                        tagStyle={tagStyle}
                        fontSize={fontSize}
                        iconSize={iconSize}
                        iconFontSize={iconFontSize}
                    />
                </DomainWrapper>
            </HoverEntityTooltip>
        );
    }

    return (
        <HoverEntityTooltip entity={domain} canOpen={enableTooltip} previewContext={previewContext}>
            <DomainLinkContainer to={entityRegistry.getEntityUrl(EntityType.Domain, urn)} {...linkProps}>
                <DomainContent
                    domain={domain}
                    name={name}
                    closable={closable}
                    onClose={onClose}
                    tagStyle={tagStyle}
                    fontSize={fontSize}
                    iconSize={iconSize}
                    iconFontSize={iconFontSize}
                />
            </DomainLinkContainer>
        </HoverEntityTooltip>
    );
};
