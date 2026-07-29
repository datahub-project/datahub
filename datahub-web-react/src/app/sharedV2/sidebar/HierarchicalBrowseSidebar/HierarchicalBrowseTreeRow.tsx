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
import {
    getTreeRowChromeFlags,
    getTreeRowPaddingLeft,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/treeRowChrome';

/**
 * Shared hierarchical-browse tree row.
 *
 * Pages own data loading + navigation; this owns visual chrome so Documents /
 * Glossary / Domains / Metrics cannot drift. SaaS badges go in `afterLabel` —
 * never fork this file for lifecycle/version chrome.
 */

const TitleContent = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
    min-width: 0;
    flex: 1;
    overflow: hidden;
`;

/** Documents: indent in ExpandZone; right edge matches shared row (2px). */
const ExpandZoneRow = styled(TreeRowContainer)`
    padding-top: 0;
    padding-bottom: 0;
    padding-right: 2px;
    padding-left: 0;
`;

const ExpandZoneLeft = styled(TreeRowLeftContent)`
    align-self: stretch;
`;

const ExpandZone = styled.div<{ $level: number; $expandable: boolean }>`
    display: flex;
    align-items: center;
    align-self: stretch;
    padding-left: ${(props) => getTreeRowPaddingLeft(props.$level)}px;
    flex-shrink: 0;
    cursor: ${(props) => (props.$expandable ? 'pointer' : 'inherit')};
`;

const ExpandZoneIconSlot = styled(TreeRowIconSlot)`
    align-self: stretch;
    height: auto;
`;

export type HierarchicalBrowseTreeRowProps = {
    level: number;
    isSelected: boolean;
    /** Sidebar collapsed column — icon only, no label/caret/count. */
    isCollapsed?: boolean;
    hasChildren?: boolean;
    isExpanded?: boolean;
    /** Shown as Pill when !isExpanded && hasChildren. */
    count?: number;
    /** Entity glyph — always visible; caret sits at the far right when expandable. */
    icon: React.ReactNode;
    label: React.ReactNode;
    labelTitle?: string;
    /**
     * Inline badges after the title (OSS deprecation, SaaS lifecycle/version).
     * Hidden when `isCollapsed`.
     */
    afterLabel?: React.ReactNode;
    /**
     * Extra right-side chrome (SelectedMark, actions). Rendered before the
     * caret. To hide the count (e.g. Documents hover actions), pass
     * `count={undefined}` while setting `trailing`.
     */
    trailing?: React.ReactNode;
    onSelect: () => void;
    onToggleExpand?: () => void;
    /** When loading children, spinner stays in the icon slot; caret remains. */
    isLoadingChildren?: boolean;
    /**
     * `row` (default): indent via TreeRowContainer padding.
     * `leadingZone`: Documents ExpandZone — full-height indent hit target.
     */
    expandHitArea?: 'row' | 'leadingZone';
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
    expandHitArea = 'row',
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

    const handleExpandZoneClick = (e: React.MouseEvent) => {
        if (!hasChildren) return;
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

    const rightChrome = showRightChrome ? (
        <TreeRowRightContent>
            {showCount && <Pill label={`${count}`} size="sm" />}
            {trailing}
            {caretSlot}
        </TreeRowRightContent>
    ) : null;

    if (expandHitArea === 'leadingZone' && !isCollapsed) {
        return (
            <ExpandZoneRow
                className={className}
                data-testid={dataTestId}
                $level={0}
                $isSelected={isSelected}
                onClick={onSelect}
                onMouseEnter={onMouseEnter}
                onMouseLeave={onMouseLeave}
            >
                <ExpandZoneLeft>
                    <ExpandZone $level={level} $expandable={hasChildren} onClick={handleExpandZoneClick}>
                        <ExpandZoneIconSlot>{entityIcon}</ExpandZoneIconSlot>
                    </ExpandZone>
                    {titleBlock}
                </ExpandZoneLeft>
                {rightChrome}
            </ExpandZoneRow>
        );
    }

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
            {rightChrome}
        </TreeRowContainer>
    );
}
