import { Tooltip } from '@components';
import { ArrowsInLineVertical } from '@phosphor-icons/react/dist/csr/ArrowsInLineVertical';
import { ArrowsOutLineVertical } from '@phosphor-icons/react/dist/csr/ArrowsOutLineVertical';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { TREE_ROW_CARET_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { TreeRowExpandButton } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/treeRow.styles';
import { getTreeRowPaddingLeft } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/treeRowChrome';

const SectionHeaderRow = styled.div<{ $level: number }>`
    display: flex;
    align-items: center;
    gap: 4px;
    width: 100%;
    padding: 6px 8px 6px ${(props) => getTreeRowPaddingLeft(props.$level)}px;
    min-height: 32px;
    color: ${(props) => props.theme.colors.textTertiary};
    font-family: Mulish;
    font-size: 14px;
    font-weight: 700;
`;

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

/** Section label + optional expand-all; carets match tree-row chrome. */
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
    const theme = useTheme();
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
                    <TreeRowExpandButton
                        type="button"
                        onClick={onToggleExpandAll}
                        disabled={expandAllLoading}
                        aria-label={bulkLabel}
                        data-testid={testId ? `${testId}-expand-all` : undefined}
                    >
                        {isAllExpanded ? (
                            <ArrowsInLineVertical
                                color={theme.colors.icon}
                                size={TREE_ROW_CARET_SIZE}
                                weight="regular"
                            />
                        ) : (
                            <ArrowsOutLineVertical
                                color={theme.colors.icon}
                                size={TREE_ROW_CARET_SIZE}
                                weight="regular"
                            />
                        )}
                    </TreeRowExpandButton>
                </Tooltip>
            )}
            <TreeRowExpandButton
                type="button"
                onClick={onToggle}
                aria-expanded={isExpanded}
                aria-label={label}
                tabIndex={-1}
            >
                <Caret color={theme.colors.icon} size={TREE_ROW_CARET_SIZE} weight="regular" />
            </TreeRowExpandButton>
        </SectionHeaderRow>
    );
}
