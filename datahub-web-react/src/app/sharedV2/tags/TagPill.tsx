/* eslint-disable rulesdir/no-hardcoded-colors */
import ColorHash from 'color-hash';
import React from 'react';
import styled, { css } from 'styled-components';

import PillRemoveIcon from '@app/sharedV2/icons/PillRemoveIcon';

// Shared color-hash instance — matches StyledTag.tsx so URN-derived colors line up with the rest of
// the app's tag chips.
const generateColor = new ColorHash({ saturation: 0.9 });

const PillRoot = styled.span<{
    $highlight?: boolean;
    $clickable?: boolean;
    $hasRemove?: boolean;
    $borderless?: boolean;
}>`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    max-width: 100%;
    min-width: 0;
    line-height: 1.4;
    color: ${(props) => props.theme.colors.textSecondary};
    font-weight: 400;
    ${(props) => props.$clickable && 'cursor: pointer;'}

    /* Borderless variant: replaces the legacy TagLabel — bare dot + name with no chip body. */
    ${(props) =>
        props.$borderless
            ? css`
                  padding: 0;
                  border: none;
                  background: transparent;
              `
            : css`
                  height: 28px;
                  box-sizing: border-box;
                  padding: 0 ${props.$hasRemove ? '6px' : '8px'} 0 8px;
                  border-radius: 100em;
                  border: 1px solid ${props.$highlight ? props.theme.colors.borderHover : props.theme.colors.border};
                  background: ${props.$highlight ? props.theme.colors.bgHighlight : props.theme.colors.bg};
              `}
`;

const ColorDot = styled.span<{ $color: string }>`
    display: inline-block;
    min-width: 8px;
    min-height: 8px;
    background: ${(props) => props.$color};
    border-radius: 100em;
`;

const PillLabel = styled.span<{ $fontSize?: number }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 200px;
    min-width: 0;
    ${(props) => props.$fontSize && `font-size: ${props.$fontSize}px;`}
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
    /** Renders in the highlight color scheme (used when search matches this tag). */
    highlight?: boolean;
    /** Override label font size in px. */
    fontSize?: number;
    /** Apply `cursor: pointer` to the pill body. */
    clickable?: boolean;
    /**
     * Drops the chip border/background/padding for inline-with-text contexts
     * (breadcrumbs, mentions, modal options). Replaces the legacy `TagLabel`.
     */
    borderless?: boolean;
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
    highlight,
    fontSize,
    clickable,
    borderless,
    className,
    dataTestId,
}: Props) {
    const dotColor = color || (colorHash ? generateColor.hex(colorHash) : generateColor.hex(name));
    return (
        <PillRoot
            $highlight={highlight}
            $clickable={clickable}
            $hasRemove={!!onRemove}
            $borderless={borderless}
            className={className}
            data-testid={dataTestId ?? `tag-${name}`}
        >
            <ColorDot $color={dotColor} />
            <PillLabel $fontSize={fontSize}>{children ?? name}</PillLabel>
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
