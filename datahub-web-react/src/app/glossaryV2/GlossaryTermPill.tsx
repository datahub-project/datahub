import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { Icon as PhosphorIcon } from '@phosphor-icons/react/dist/lib/types';
import React from 'react';
import styled, { css } from 'styled-components';

import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import PillRemoveIcon from '@app/sharedV2/icons/PillRemoveIcon';

const PillRoot = styled.span<{
    $highlight?: boolean;
    $clickable?: boolean;
    $proposed?: boolean;
    $hasRemove?: boolean;
    $borderless?: boolean;
}>`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    max-width: 100%;
    min-width: 0;
    line-height: 1.4;
    color: ${(props) => (props.$proposed ? props.theme.colors.textSecondary : props.theme.colors.text)};
    font-weight: 400;
    ${(props) => props.$clickable && 'cursor: pointer;'}

    /* Borderless variant: replaces the legacy TermLabel — bare icon + name with no chip body. */
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
                  padding: 0 ${props.$hasRemove ? '4px' : '8px'} 0 3px;
                  border-radius: 5px;
                  /* Proposed terms (SaaS proposal flow) render with a dashed border to signal pending approval. */
                  border: 1px ${props.$proposed ? 'dashed' : 'solid'}
                      ${props.$highlight ? props.theme.colors.borderHover : props.theme.colors.border};
                  background: ${props.$highlight ? props.theme.colors.bgHighlight : props.theme.colors.bg};
              `}
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
    /** Renders in the highlight color scheme (used when search matches this term). */
    highlight?: boolean;
    /**
     * Renders the pill in the "proposed" state — dashed border and secondary text color.
     * Used by the SaaS proposal flow to indicate a term that has been suggested but not yet approved.
     */
    proposed?: boolean;
    /** Override label font size in px. */
    fontSize?: number;
    /** Apply `cursor: pointer` to the pill body. */
    clickable?: boolean;
    /**
     * Drops the chip border/background/padding for inline-with-text contexts
     * (breadcrumbs, mentions, modal options). Replaces the legacy `TermLabel`.
     * Also shrinks the leading icon container to read at body-text scale.
     */
    borderless?: boolean;
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
    highlight,
    proposed,
    fontSize,
    clickable,
    borderless,
    className,
    dataTestId,
}: Props) {
    // Inline (borderless) usages sit alongside body text, so the icon container shrinks to read at
    // the same visual weight as a single character.
    const iconBoxSize = borderless ? 16 : 20;
    const iconGlyphSize = borderless ? 10 : 12;
    return (
        <PillRoot
            $highlight={highlight}
            $proposed={proposed}
            $clickable={clickable}
            $hasRemove={!!onRemove}
            $borderless={borderless}
            className={className}
            data-testid={dataTestId ?? `term-${name}`}
        >
            <GlossaryColoredIcon color={color} icon={icon} size={iconBoxSize} iconSize={iconGlyphSize} radius={3} />
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
