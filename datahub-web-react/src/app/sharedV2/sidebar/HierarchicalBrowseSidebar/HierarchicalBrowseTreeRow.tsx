import { Pill } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { TREE_ROW_CARET_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import {
    TreeRowCaretSlot,
    TreeRowContainer,
    TreeRowExpandButton,
    TreeRowIconSlot,
    TreeRowLeftContent,
    TreeRowRightContent,
    TreeRowTitle,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/treeRow.styles';
import { getTreeRowChromeFlags } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/treeRowChrome';

/**
 * Shared hierarchical-browse tree row. Pages own data/nav; this owns chrome.
 * SaaS badges → `afterLabel` (do not fork this file).
 */

const TitleContent = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    flex: 1;
    overflow: hidden;
`;

export type HierarchicalBrowseTreeRowProps = {
    level: number;
    isSelected: boolean;
    isCollapsed?: boolean;
    hasChildren?: boolean;
    isExpanded?: boolean;
    count?: number;
    icon: React.ReactNode;
    label: React.ReactNode;
    labelTitle?: string;
    afterLabel?: React.ReactNode;
    trailing?: React.ReactNode;
    onSelect: () => void;
    onToggleExpand?: () => void;
    isLoadingChildren?: boolean;
    'data-testid'?: string;
    className?: string;
    onMouseEnter?: () => void;
    onMouseLeave?: () => void;
};

export default function HierarchicalBrowseTreeRow({
    level,
    isSelected,
    isCollapsed = false,
    hasChildren = false,
    isExpanded = false,
    count,
    icon,
    label,
    labelTitle,
    afterLabel,
    trailing,
    onSelect,
    onToggleExpand,
    isLoadingChildren = false,
    'data-testid': dataTestId,
    className,
    onMouseEnter,
    onMouseLeave,
}: HierarchicalBrowseTreeRowProps) {
    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();

    const { canExpand, showCount, showRightChrome, reserveCaretSlot } = getTreeRowChromeFlags({
        isCollapsed,
        hasChildren,
        isExpanded,
        count,
        hasToggle: onToggleExpand != null,
    });

    const handleExpandClick = (e: React.MouseEvent) => {
        e.stopPropagation();
        onToggleExpand?.();
    };

    const caretSlot = reserveCaretSlot ? (
        <TreeRowCaretSlot>
            {canExpand ? (
                <TreeRowExpandButton
                    type="button"
                    onClick={handleExpandClick}
                    aria-expanded={isExpanded}
                    aria-label={isExpanded ? tc('collapse') : tc('expand')}
                >
                    {isExpanded ? (
                        <CaretDown color={theme.colors.icon} size={TREE_ROW_CARET_SIZE} weight="regular" />
                    ) : (
                        <CaretRight color={theme.colors.icon} size={TREE_ROW_CARET_SIZE} weight="regular" />
                    )}
                </TreeRowExpandButton>
            ) : null}
        </TreeRowCaretSlot>
    ) : null;

    const entityIcon = isLoadingChildren ? (
        <TreeRowExpandButton type="button" onClick={handleExpandClick} aria-label={tc('expand')}>
            {icon}
        </TreeRowExpandButton>
    ) : (
        icon
    );

    const titleBlock =
        afterLabel != null ? (
            <TitleContent>
                <TreeRowTitle $isSelected={isSelected} title={labelTitle}>
                    {label}
                </TreeRowTitle>
                {afterLabel}
            </TitleContent>
        ) : (
            <TreeRowTitle $isSelected={isSelected} title={labelTitle}>
                {label}
            </TreeRowTitle>
        );

    return (
        <TreeRowContainer
            className={className}
            data-testid={dataTestId}
            $level={level}
            $isSelected={isSelected}
            $isCollapsed={isCollapsed}
            onClick={onSelect}
            onMouseEnter={onMouseEnter}
            onMouseLeave={onMouseLeave}
        >
            <TreeRowLeftContent $isCollapsed={isCollapsed}>
                <TreeRowIconSlot $isCollapsed={isCollapsed}>{entityIcon}</TreeRowIconSlot>
                {!isCollapsed && titleBlock}
            </TreeRowLeftContent>
            {showRightChrome && (
                <TreeRowRightContent>
                    {showCount && <Pill label={`${count}`} size="sm" />}
                    {trailing}
                    {caretSlot}
                </TreeRowRightContent>
            )}
        </TreeRowContainer>
    );
}
