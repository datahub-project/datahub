import type { Icon as PhosphorIcon } from '@phosphor-icons/react';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

// Mirrors `TreeItemContainer` in
// `app/homeV2/layout/sidebar/documents/DocumentTreeItem.tsx` (jjoyce0510):
//   - 38px min-height + height, 4px vertical padding
//   - 2px lateral / bottom margin
//   - `bgSelectedSubtle` + `shadowFocusBrand` on selected
//   - `bgHover` + `shadowFocus` on hover
//
// Rendered as a `<div>` (not `<button>`) for the same reason Joyce did:
// the row contains a nested expand `<button>`, and nesting buttons is
// invalid HTML (browsers tolerate it, but screen readers and a11y tools
// will warn). `$level` is a plain `number` so deeper trees (semantic
// model → metric → metric child) don't need a type widening later.
const TreeItemContainer = styled.div<{ $level: number; $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
    min-height: 38px;
    height: 38px;
    margin: 0 2px 2px 2px;
    border-radius: 6px;
    background: ${(props) => (props.$isSelected ? props.theme.colors.bgSelectedSubtle : 'transparent')};
    cursor: pointer;
    transition: background-color 0.15s ease;

    ${(props) => props.$isSelected && `box-shadow: ${props.theme.colors.shadowFocusBrand};`}

    ${(props) =>
        !props.$isSelected &&
        `
        &:hover {
            background: ${props.theme.colors.bgHover};
            box-shadow: ${props.theme.colors.shadowFocus};
        }
    `}
`;

const LeftContent = styled.div`
    display: flex;
    align-items: center;
    flex: 1;
    min-width: 0;
    overflow: hidden;
`;

// Single 20x20 slot that hosts either the row's resting icon or the caret
// when the row is hover-expanded — same pattern as `IconSlot` in Documents.
// Margin-right 8px matches `gap`-equivalent spacing in DocumentTreeItem.
const IconSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    margin-right: 8px;
    flex-shrink: 0;
`;

// Caret swap button. Stops click propagation so toggling expand doesn't
// also fire the row's onClick (which would navigate). Identical contract
// to `ExpandButton` in `DocumentTreeItem`.
const ExpandButton = styled.button`
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

    &:hover {
        opacity: 0.7;
    }
`;

// Resting icon: `theme.colors.icon` by default, `theme.colors.iconBrand`
// when selected. Same treatment as `IconWrapper` in DocumentTreeItem
// (minus the gradient fill, which is reserved for the title text).
const IconWrapper = styled.div<{ $isSelected: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    flex-shrink: 0;

    && svg {
        color: ${(props) => (props.$isSelected ? props.theme.colors.iconBrand : props.theme.colors.icon)};
    }
`;

// Row label. Default = `textSecondary`. Selected = brand gradient clipped
// to the text + `font-weight: 600` — identical to `Title` in DocumentTreeItem.
const Title = styled.span<{ $isSelected: boolean }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 14px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};

    ${(props) =>
        props.$isSelected &&
        `
        background: ${props.theme.colors.brandGradientSelected};
        background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 600;
    `}
`;

interface MetricsTreeItemProps {
    /** Indentation level — 0 = root, increments per nesting depth. */
    level: number;
    /** Phosphor icon component for the row's resting state. */
    icon: PhosphorIcon;
    title: string;
    isSelected: boolean;
    /** When true, the icon swaps to a caret on hover (and stays as a caret while expanded). */
    hasChildren?: boolean;
    isExpanded?: boolean;
    onClick: () => void;
    onToggleExpand?: () => void;
    testId?: string;
}

/**
 * MetricsTreeItem - single row in the Metrics sidebar tree.
 *
 * Behaviour deliberately mirrors `DocumentTreeItem` (jjoyce0510 — at
 * `app/homeV2/layout/sidebar/documents/DocumentTreeItem.tsx`) so the two
 * sidebars feel identical to use:
 *   - one icon slot, contents swap between resting icon and caret
 *   - caret appears on hover when `hasChildren`, or while `isExpanded`
 *   - clicking the caret toggles expand without firing the row's onClick
 *   - selected rows get `bgSelectedSubtle` + `shadowFocusBrand` chrome and
 *     the brand-gradient-clipped title; selected icons render in `iconBrand`
 *     and switch from `regular` to `fill` weight
 *
 * Implemented as a separate component (not a fork of DocumentTreeItem)
 * because Documents' row is coupled to document-specific affordances
 * (dashed icons for unpublished, platform-logo for external, hover-row
 * actions menu) that don't apply to metrics. If the row chrome
 * stabilises across both we can lift a shared primitive.
 *
 * Currently used by `MetricsSidebar` only for the Overview row (no
 * children). When `EntityType.SemanticModel` / `EntityType.Metric` land
 * the same component will render semantic-model rows (`hasChildren`)
 * and metric leaf rows with no further refactoring.
 */
export const MetricsTreeItem: React.FC<MetricsTreeItemProps> = ({
    level,
    icon: Icon,
    title,
    isSelected,
    hasChildren = false,
    isExpanded = false,
    onClick,
    onToggleExpand,
    testId,
}) => {
    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
    const [isHovered, setIsHovered] = useState(false);

    const showCaret = hasChildren && (isExpanded || isHovered);

    const handleExpandClick = (e: React.MouseEvent) => {
        e.stopPropagation();
        onToggleExpand?.();
    };

    return (
        <TreeItemContainer
            data-testid={testId}
            $level={level}
            $isSelected={isSelected}
            onClick={onClick}
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
        >
            <LeftContent>
                <IconSlot>
                    {showCaret ? (
                        <ExpandButton
                            onClick={handleExpandClick}
                            aria-label={isExpanded ? tc('collapse') : tc('expand')}
                        >
                            {isExpanded ? (
                                <CaretDown color={theme.colors.icon} size={16} weight="bold" />
                            ) : (
                                <CaretRight color={theme.colors.icon} size={16} weight="bold" />
                            )}
                        </ExpandButton>
                    ) : (
                        <IconWrapper $isSelected={isSelected}>
                            <Icon size={20} weight={isSelected ? 'fill' : 'regular'} />
                        </IconWrapper>
                    )}
                </IconSlot>

                <Title $isSelected={isSelected} title={title}>
                    {title}
                </Title>
            </LeftContent>
        </TreeItemContainer>
    );
};
