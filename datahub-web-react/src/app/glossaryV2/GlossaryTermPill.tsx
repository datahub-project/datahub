import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { Icon as PhosphorIcon } from '@phosphor-icons/react/dist/lib/types';
import React from 'react';
import styled, { css } from 'styled-components';

import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import PillRemoveIcon from '@app/sharedV2/icons/PillRemoveIcon';

/**
 * `default` — standard chip used in entity sidebars and modal selections.
 * `borderless` — inline-with-text usage (breadcrumbs, mentions, modal option rows). No chip body.
 * `highlighted` — chip is the active search match. Uses the hover border/background tokens.
 */
export type GlossaryTermPillVariant = 'default' | 'borderless' | 'highlighted';

/** `md` is the canonical sidebar/modal size; `sm` is the compact size used in dense filter rows. */
export type GlossaryTermPillSize = 'sm' | 'md';

const PillRoot = styled.span<{
    $variant: GlossaryTermPillVariant;
    $clickable?: boolean;
    $hasRemove?: boolean;
}>`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    max-width: 100%;
    min-width: 0;
    line-height: 1.4;
    color: ${(props) => props.theme.colors.text};
    font-weight: 500;
    ${(props) => props.$clickable && 'cursor: pointer;'}

    ${(props) =>
        props.$variant === 'borderless'
            ? css`
                  padding: 0;
                  border: none;
                  background: transparent;
              `
            : css`
                  height: 26px;
                  box-sizing: border-box;
                  padding: 0 ${props.$hasRemove ? '4px' : '8px'} 0 3px;
                  border-radius: 5px;
                  border: 1px solid
                      ${props.$variant === 'highlighted' ? props.theme.colors.borderHover : props.theme.colors.border};
                  background: ${props.$variant === 'highlighted'
                      ? props.theme.colors.bgHighlight
                      : props.theme.colors.bg};
              `}
`;

const PillLabel = styled.span<{ $size: GlossaryTermPillSize }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 200px;
    min-width: 0;
    font-size: ${(props) => (props.$size === 'sm' ? '10px' : '12px')};
`;

interface Props {
    /** Plain-text term name — rendered as the label unless `children` is provided. */
    name: string;
    /** Hex color used by the leading colored-icon container (root glossary node's color). */
    color: string;
    /** Phosphor icon to render inside the colored container. Defaults to BookmarkSimple (single bookmark). */
    icon?: PhosphorIcon;
    /** Optional override for the label cell — e.g. a `<Highlight>` for search matches. */
    children?: React.ReactNode;
    /** Optional slot rendered between the label and the remove icon — e.g. a propagation indicator. */
    rightAdornment?: React.ReactNode;
    /** When provided, shows a Phosphor X close icon and calls back on click. */
    onRemove?: (e: React.MouseEvent<SVGSVGElement>) => void;
    /** Visual variant. See `GlossaryTermPillVariant` for semantics. */
    variant?: GlossaryTermPillVariant;
    /** Pill size. `md` is the default sidebar/modal size; `sm` is used in compact filter rows. */
    size?: GlossaryTermPillSize;
    /** Apply `cursor: pointer` to the pill body. */
    clickable?: boolean;
    className?: string;
    dataTestId?: string;
}

export default function GlossaryTermPill({
    name,
    color,
    icon = BookmarkSimple,
    children,
    rightAdornment,
    onRemove,
    variant = 'default',
    size = 'md',
    clickable,
    className,
    dataTestId,
}: Props) {
    // Borderless usages sit alongside body text, so the icon container shrinks to read at the same
    // visual weight as a single character.
    const isBorderless = variant === 'borderless';
    const iconBoxSize = isBorderless ? 16 : 20;
    const iconGlyphSize = isBorderless ? 10 : 12;
    return (
        <PillRoot
            $variant={variant}
            $clickable={clickable}
            $hasRemove={!!onRemove}
            className={className}
            data-testid={dataTestId ?? `term-${name}`}
        >
            <GlossaryColoredIcon color={color} icon={icon} size={iconBoxSize} iconSize={iconGlyphSize} radius={3} />
            <PillLabel $size={size}>{children ?? name}</PillLabel>
            {rightAdornment}
            {onRemove && (
                <PillRemoveIcon
                    onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        onRemove(e);
                    }}
                />
            )}
        </PillRoot>
    );
}
