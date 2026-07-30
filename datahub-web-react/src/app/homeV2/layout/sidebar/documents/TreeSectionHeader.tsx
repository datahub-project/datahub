import { ArrowsInLineVertical } from '@phosphor-icons/react/dist/csr/ArrowsInLineVertical';
import { ArrowsOutLineVertical } from '@phosphor-icons/react/dist/csr/ArrowsOutLineVertical';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import styled from 'styled-components';

import { Tooltip } from '@src/alchemy-components';

// Section header for the DataHub / per-platform group rows. Indent mirrors
// DocumentTreeItem (8 + level*16) so a level-1 platform header lines up with a
// level-1 doc row. The controls (expand-all + chevron) sit on the right to
// signal that this row is a structural group, not an interactive doc.
// No hover background — it's a tree label, not a nav row.
const SectionHeaderRow = styled.div<{ $level: number }>`
    display: flex;
    align-items: center;
    gap: 4px;
    width: 100%;
    padding: 6px 2px 6px ${(props) => 8 + props.$level * 16}px;
    min-height: 32px;
    color: ${(props) => props.theme.colors.textTertiary};
    font-family: Mulish;
    font-size: 14px;
    font-weight: 700;
`;

// Fills the row up to the trailing controls so the whole label area toggles the
// section. Transparent, borderless — it reads as a label, not a button.
const SectionToggleButton = styled.button`
    display: flex;
    align-items: center;
    gap: 8px;
    flex: 1;
    min-width: 0;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
    text-align: left;
    color: inherit;
    font: inherit;
`;

const SectionHeaderLabel = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

const SectionIconButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: ${(props) => props.theme.colors.icon};
    flex-shrink: 0;

    &:hover {
        opacity: 0.7;
    }

    &:disabled {
        cursor: default;
        opacity: 0.5;
    }
`;

interface TreeSectionHeaderProps {
    level: number;
    label: string;
    icon?: React.ReactNode;
    isExpanded: boolean;
    onToggle: () => void;
    testId?: string;
    onToggleExpandAll?: () => void;
    isAllExpanded?: boolean;
    expandAllLoading?: boolean;
    expandAllLabel?: string;
    collapseAllLabel?: string;
}

/**
 * Collapsible header row for a tree section (DataHub or a per-platform
 * sub-section). Pure presentation — owns no expansion state of its own.
 *
 * When `onToggleExpandAll` is provided, a bulk expand/collapse control renders
 * next to the section chevron. `isAllExpanded` picks the glyph (arrows-in =
 * collapse everything in this section, arrows-out = expand everything).
 */
export function TreeSectionHeader({
    level,
    label,
    icon,
    isExpanded,
    onToggle,
    testId,
    onToggleExpandAll,
    isAllExpanded = false,
    expandAllLoading = false,
    expandAllLabel,
    collapseAllLabel,
}: TreeSectionHeaderProps) {
    const Caret = isExpanded ? CaretDown : CaretRight;
    const bulkLabel = isAllExpanded ? collapseAllLabel : expandAllLabel;
    return (
        <SectionHeaderRow $level={level} data-testid={testId}>
            <SectionToggleButton type="button" onClick={onToggle} aria-expanded={isExpanded}>
                <SectionHeaderLabel>
                    {icon}
                    {label}
                </SectionHeaderLabel>
            </SectionToggleButton>
            {onToggleExpandAll && (
                <Tooltip title={bulkLabel} placement="bottom" showArrow={false}>
                    <SectionIconButton
                        type="button"
                        onClick={onToggleExpandAll}
                        disabled={expandAllLoading}
                        aria-label={bulkLabel}
                        data-testid={testId ? `${testId}-expand-all` : undefined}
                    >
                        {isAllExpanded ? (
                            <ArrowsInLineVertical size={16} weight="regular" />
                        ) : (
                            <ArrowsOutLineVertical size={16} weight="regular" />
                        )}
                    </SectionIconButton>
                </Tooltip>
            )}
            <SectionIconButton
                type="button"
                onClick={onToggle}
                aria-expanded={isExpanded}
                aria-label={label}
                tabIndex={-1}
            >
                <Caret size={16} weight="regular" />
            </SectionIconButton>
        </SectionHeaderRow>
    );
}
