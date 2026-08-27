import { Pill, Tooltip } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { TREE_ROW_CARET_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import {
    TreeRowContainer,
    TreeRowCount,
    TreeRowExpandZone,
    TreeRowIconSlot,
    TreeRowLeadingExpand,
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
    /** Documents use `hover`; Glossary / Domains keep `always`. */
    countReveal?: 'always' | 'hover';
    onSelect: () => void;
    onToggleExpand?: () => void;
    isLoadingChildren?: boolean;
    'data-testid'?: string;
    className?: string;
};

const HierarchicalBrowseTreeRow = React.forwardRef<HTMLDivElement, HierarchicalBrowseTreeRowProps>(
    (
        {
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
            countReveal = 'always',
            onSelect,
            onToggleExpand,
            isLoadingChildren = false,
            'data-testid': dataTestId,
            className,
        },
        ref,
    ) => {
        const { t: tc } = useTranslation('common.actions');
        const theme = useTheme();

        const { canExpand, showCount } = getTreeRowChromeFlags({
            isCollapsed,
            hasChildren,
            isExpanded,
            count,
            hasToggle: onToggleExpand != null,
        });
        const showRightChrome = !isCollapsed && (showCount || trailing != null);

        // Single expand path for indent zone + caret button (stopPropagation so row select doesn't fire).
        const handleExpand = (e: React.MouseEvent) => {
            if (!canExpand) return;
            e.stopPropagation();
            onToggleExpand?.();
        };

        const caret = isExpanded ? (
            <CaretDown color={theme.colors.icon} size={TREE_ROW_CARET_SIZE} weight="regular" />
        ) : (
            <CaretRight color={theme.colors.icon} size={TREE_ROW_CARET_SIZE} weight="regular" />
        );

        // Loading: keep both CSS slots on the same glyph so hover swap doesn't thrash mid-fetch.
        let leading: React.ReactNode = icon;
        if (canExpand) {
            const caretGlyph = isLoadingChildren ? icon : caret;
            leading = (
                <TreeRowLeadingExpand
                    type="button"
                    $isExpanded={isExpanded}
                    onClick={handleExpand}
                    aria-expanded={isExpanded}
                    aria-label={isExpanded ? tc('collapse') : tc('expand')}
                >
                    <span className="tree-row-entity-icon">{icon}</span>
                    <span className="tree-row-caret-icon">{caretGlyph}</span>
                </TreeRowLeadingExpand>
            );
        }

        // Native `title` on truncated text is unreliable (esp. nested flex); use Tooltip.
        const titleEl = <TreeRowTitle $isSelected={isSelected}>{label}</TreeRowTitle>;
        const titledLabel =
            labelTitle != null && labelTitle !== '' ? (
                <Tooltip
                    title={labelTitle}
                    placement="right"
                    mouseEnterDelay={0.1}
                    mouseLeaveDelay={0}
                    showArrow={false}
                >
                    {titleEl}
                </Tooltip>
            ) : (
                titleEl
            );

        const titleBlock =
            afterLabel != null ? (
                <TitleContent>
                    {titledLabel}
                    {afterLabel}
                </TitleContent>
            ) : (
                titledLabel
            );

        return (
            <TreeRowContainer
                ref={ref}
                className={className}
                data-testid={dataTestId}
                $isSelected={isSelected}
                $isCollapsed={isCollapsed}
                onClick={onSelect}
            >
                <TreeRowLeftContent $isCollapsed={isCollapsed}>
                    {isCollapsed ? (
                        <TreeRowIconSlot $isCollapsed>{leading}</TreeRowIconSlot>
                    ) : (
                        <TreeRowExpandZone $level={level} $expandable={canExpand} onClick={handleExpand}>
                            <TreeRowIconSlot>{leading}</TreeRowIconSlot>
                        </TreeRowExpandZone>
                    )}
                    {!isCollapsed && titleBlock}
                </TreeRowLeftContent>
                {showRightChrome && (
                    <TreeRowRightContent>
                        {showCount &&
                            (countReveal === 'hover' ? (
                                <TreeRowCount>
                                    <Pill label={`${count}`} size="sm" />
                                </TreeRowCount>
                            ) : (
                                <Pill label={`${count}`} size="sm" />
                            ))}
                        {trailing}
                    </TreeRowRightContent>
                )}
            </TreeRowContainer>
        );
    },
);
HierarchicalBrowseTreeRow.displayName = 'HierarchicalBrowseTreeRow';

export default HierarchicalBrowseTreeRow;
