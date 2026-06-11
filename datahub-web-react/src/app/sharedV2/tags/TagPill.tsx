import ColorHash from 'color-hash';
import React from 'react';
import styled, { css } from 'styled-components';

import PillRemoveIcon from '@app/sharedV2/icons/PillRemoveIcon';

/**
 * `default` — standard chip used in entity sidebars and modal selections.
 * `borderless` — inline-with-text usage (breadcrumbs, mentions, modal option rows). No chip body.
 * `highlighted` — chip is the active search match. Uses the hover border/background tokens.
 */
export type TagPillVariant = 'default' | 'borderless' | 'highlighted';

/** `md` is the canonical sidebar/modal size; `sm` is the compact size used in dense filter rows. */
export type TagPillSize = 'sm' | 'md';

// Shared color-hash instance — matches StyledTag.tsx so URN-derived colors line up with the rest of
// the app's tag chips.
// eslint-disable-next-line rulesdir/no-hardcoded-colors
const generateColor = new ColorHash({ saturation: 0.9 });

const PillRoot = styled.span<{
    $variant: TagPillVariant;
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
                  padding: 0 ${props.$hasRemove ? '6px' : '8px'} 0 8px;
                  border-radius: 100em;
                  border: 1px solid
                      ${props.$variant === 'highlighted' ? props.theme.colors.borderHover : props.theme.colors.border};
                  background: ${props.$variant === 'highlighted'
                      ? props.theme.colors.bgHighlight
                      : props.theme.colors.bg};
              `}
`;

const ColorDot = styled.span<{ $color: string }>`
    display: inline-block;
    min-width: 8px;
    min-height: 8px;
    background: ${(props) => props.$color};
    border-radius: 100em;
`;

const PillLabel = styled.span<{ $size: TagPillSize }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 200px;
    min-width: 0;
    font-size: ${(props) => (props.$size === 'sm' ? '10px' : '12px')};
`;

interface Props {
    /** Plain-text tag name — rendered as the label unless `children` is provided. */
    name: string;
    /** Explicit color hex from the tag's `properties.colorHex`. Falls back to a URN-hashed color. */
    color?: string | null;
    /** URN (or any stable string) used to deterministically derive a color when `color` is omitted. */
    colorHash?: string;
    /** Optional override for the label cell — e.g. a `<Highlight>` for search matches. */
    children?: React.ReactNode;
    /** Optional slot rendered between the label and the remove icon. */
    rightAdornment?: React.ReactNode;
    /** When provided, shows a Phosphor X close icon and calls back on click. */
    onRemove?: (e: React.MouseEvent<SVGSVGElement>) => void;
    /** Visual variant. See `TagPillVariant` for semantics. */
    variant?: TagPillVariant;
    /** Pill size. `md` is the default sidebar/modal size; `sm` is used in compact filter rows. */
    size?: TagPillSize;
    /** Apply `cursor: pointer` to the pill body. */
    clickable?: boolean;
    className?: string;
    dataTestId?: string;
}

export default function TagPill({
    name,
    color,
    colorHash,
    children,
    rightAdornment,
    onRemove,
    variant = 'default',
    size = 'md',
    clickable,
    className,
    dataTestId,
}: Props) {
    const dotColor = color || (colorHash ? generateColor.hex(colorHash) : generateColor.hex(name));
    return (
        <PillRoot
            $variant={variant}
            $clickable={clickable}
            $hasRemove={!!onRemove}
            className={className}
            data-testid={dataTestId ?? `tag-${name}`}
        >
            <ColorDot $color={dotColor} />
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
