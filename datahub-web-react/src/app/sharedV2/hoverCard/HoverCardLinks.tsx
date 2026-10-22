import { Icon, Text } from '@components';
import { ArrowUpRight } from '@phosphor-icons/react/dist/csr/ArrowUpRight';
import React from 'react';
import styled from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import useExternalLinks from '@app/entityV2/shared/externalUrl/useExternalLinks';
import usePlatformLinks from '@app/entityV2/shared/externalUrl/usePlatformLinks';
import { safeUrl } from '@app/shared/urlUtils';

import { Entity } from '@types';

// Negative margin cancels the buttons' horizontal padding so their labels line up with the
// left edge of the section content above, while keeping the padded hover background.
const Row = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    margin-left: -8px;
`;

// Mirrors the alchemy text-variant Button, as an anchor so the URL shows in the status bar and
// middle-click / copy-link keep working.
const LinkButton = styled.a`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 4px 8px;
    border-radius: 6px;
    color: ${(props) => props.theme.colors.textBrand};
    white-space: nowrap;

    &:hover,
    &:focus-visible {
        color: ${(props) => props.theme.colors.textBrand};
        background: ${(props) => props.theme.colors.bgSelectedSubtle};
    }
`;

export type HoverCardLink = {
    href: string;
    label: string;
    onClick?: () => void;
};

type Props = {
    entity: Entity;
    properties: GenericEntityProperties | null;
    /** Links the card adds itself, e.g. "View Related Assets" for a glossary term. */
    extraLinks?: HoverCardLink[];
};

/**
 * The card's outbound links, as the last section: "View in <platform>" for the entity and its siblings,
 * any institutional-memory links flagged for previews, and whatever the caller adds. All open in a
 * new tab; the card is a hover surface, so navigating the page underneath would be jarring.
 */
export default function HoverCardLinks({ entity, properties, extraLinks = [] }: Props) {
    const externalLinks = useExternalLinks(entity.urn, properties);
    const platformLinks = usePlatformLinks(entity.urn, properties, undefined, '', undefined);

    const links: HoverCardLink[] = [
        ...platformLinks.map((link) => ({ href: link.url, label: link.label, onClick: link.onClick })),
        ...externalLinks.map((link) => ({ href: link.url, label: link.label, onClick: link.onClick })),
        ...extraLinks,
    ];

    if (links.length === 0) return null;

    return (
        <Row>
            {links.map((link) => (
                <LinkButton
                    key={`${link.href}-${link.label}`}
                    href={safeUrl(link.href)}
                    target="_blank"
                    rel="noreferrer noopener"
                    onClick={link.onClick}
                >
                    <Text size="sm" weight="semiBold" color="inherit">
                        {link.label}
                    </Text>
                    <Icon icon={ArrowUpRight} size="sm" color="inherit" />
                </LinkButton>
            ))}
        </Row>
    );
}
