import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
import { pickTreeIcon } from '@app/document/utils/documentUtils';
import { DocumentActionsMenu } from '@app/homeV2/layout/sidebar/documents/DocumentActionsMenu';
import Loading from '@app/shared/Loading';
import { Button, Checkbox, Tooltip } from '@src/alchemy-components';

import { DataPlatform } from '@types';

const TreeItemContainer = styled.div<{ $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    justify-content: space-between;
    /* No vertical padding: the row's full height is a hit area so the ExpandZone
       (which stretches edge-to-edge) catches clicks in the space above/below the
       folder icon instead of the row's navigate handler. */
    padding: 0 2px 0 0;
    min-height: 38px;
    height: 38px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.15s ease;
    margin-bottom: 2px;

    ${(props) =>
        props.$isSelected &&
        `
background: ${props.theme.colors.bgSelectedSubtle};
        box-shadow: ${props.theme.colors.shadowFocusBrand};
 `}

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
    align-self: stretch;
    flex: 1;
    min-width: 0;
    overflow: hidden;
`;

// The whole left region — the indentation plus the icon/arrow — is the
// expand/collapse tap target for folders. It carries the level indentation (moved
// off the row container) and stretches to full row height so the hit area is
// generous, while the title beyond it stays a navigation target.
const ExpandZone = styled.div<{ $level: number; $expandable: boolean }>`
    display: flex;
    align-items: center;
    align-self: stretch;
    padding-left: ${(props) => 8 + props.$level * 16}px;
    flex-shrink: 0;
    cursor: ${(props) => (props.$expandable ? 'pointer' : 'inherit')};
`;

const IconSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    align-self: stretch;
    width: 24px;
    margin-right: 8px;
    flex-shrink: 0;
`;

const ExpandButton = styled.button<{ $isVisible: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: inherit;
    visibility: ${(props) => (props.$isVisible ? 'visible' : 'hidden')};

    &:hover {
        opacity: 0.7;
    }
`;

// Dashed (draft/proposed) icons can't use the selected-state gradient: it paints the icon body
// solid via `fill: url(...)`, which visually erases the dashed outline. They fall back to the
// brand color so the dash pattern stays visible.
const IconWrapper = styled.div<{ $isSelected: boolean; $useGradientFill: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    flex-shrink: 0;

    && svg {
        ${(props) => {
            if (!props.$isSelected) return `color: ${props.theme.colors.icon};`;
            if (props.$useGradientFill)
                return `fill: url(#menu-item-selected-gradient) ${props.theme.colors.iconBrand};`;
            return `color: ${props.theme.colors.iconBrand};`;
        }}
    }
`;

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

const Actions = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    margin-left: 8px;
    flex-shrink: 0;
`;

const ActionButton = styled(Button)`
    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
`;

const CheckboxSlot = styled.div`
    display: flex;
    align-items: center;
    margin-left: 8px;
    flex-shrink: 0;
`;

interface DocumentTreeItemProps {
    urn: string;
    title: string;
    level: number;
    hasChildren: boolean;
    isExpanded: boolean;
    isSelected: boolean;
    isLoading?: boolean;
    isUnpublished?: boolean; // Any non-PUBLISHED state — renders the dashed Phosphor variant (native docs only)
    isExternal?: boolean; // External-source doc — renders the platform logo (via DocumentSourceLogo) instead of the Phosphor folder/file
    platform?: DataPlatform | null; // Source platform; only consumed when isExternal is true
    onToggleExpand: () => void;
    onClick: () => void;
    onCreateChild: (parentUrn: string) => void;
    hideActions?: boolean;
    hideActionsMenu?: boolean; // Hide move/delete menu actions
    hideCreate?: boolean; // Hide create/add button
    parentUrn?: string | null;
    /**
     * When true, renders a leading checkbox and treats the row as a multi-select
     * target: `isSelected` drives the checkbox's checked state, and clicking anywhere
     * on the row (or the checkbox itself) fires `onClick` so the parent can toggle
     * the URN in its own selection set. Row actions (menu, create-child) are hidden
     * in this mode to keep the picker focused on selection.
     */
    multiSelect?: boolean;
}

export const DocumentTreeItem: React.FC<DocumentTreeItemProps> = ({
    urn,
    title,
    level,
    hasChildren,
    isExpanded,
    isSelected,
    isLoading,
    isUnpublished = false,
    isExternal = false,
    platform = null,
    onToggleExpand,
    onClick,
    onCreateChild,
    hideActions = false,
    hideActionsMenu = false,
    hideCreate = false,
    parentUrn,
    multiSelect = false,
}) => {
    const { t } = useTranslation('home.v2');
    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
    const [isHovered, setIsHovered] = useState(false);
    const [forceShowActions, setForceShowActions] = useState(false);

    const handleExpandClick = (e: React.MouseEvent) => {
        e.stopPropagation();
        onToggleExpand();
    };

    const handleAddChildClick = (e: React.MouseEvent) => {
        e.stopPropagation();
        onCreateChild(urn);
    };

    const handleItemClick = (e: React.MouseEvent) => {
        // Don't navigate if clicking on actions
        if ((e.target as HTMLElement).closest('.tree-item-actions')) {
            return;
        }
        onClick();
    };

    // The left zone (indent + icon/arrow) expands folders in place. Leaf rows have
    // nothing to expand, so we let the click bubble up to the row and open the
    // document instead.
    const handleExpandZoneClick = (e: React.MouseEvent) => {
        if (!hasChildren) return;
        e.stopPropagation();
        onToggleExpand();
    };

    const showExpandButton = hasChildren && (isExpanded || isHovered);

    const renderIcon = () => {
        if (showExpandButton) {
            return (
                <ExpandButton
                    className="tree-item-expand-button"
                    data-testid={`document-tree-expand-button-${urn}`}
                    $isVisible
                    onClick={handleExpandClick}
                    aria-label={isExpanded ? tc('collapse') : tc('expand')}
                >
                    {isLoading && <Loading height={16} marginTop={0} alignItems="center" />}
                    {!isLoading && isExpanded && <CaretDown color={theme.colors.icon} size={16} weight="bold" />}
                    {!isLoading && !isExpanded && <CaretRight color={theme.colors.icon} size={16} weight="bold" />}
                </ExpandButton>
            );
        }

        // External docs render the source platform's logo (transparent, no color extraction —
        // see DocumentSourceLogo and the PluginLogo precedent). Hover-to-caret behavior above
        // is preserved because `showExpandButton` evaluates before this branch — only the
        // resting-state glyph changes.
        if (isExternal && platform) {
            // Fallback when no logoUrl can be resolved is the regular folder/file glyph —
            // visually consistent with the rest of the tree if the platform is unknown.
            const FallbackIcon = pickTreeIcon({ hasChildren, isUnpublished: false });
            return (
                <IconWrapper className="tree-item-icon" $isSelected={false} $useGradientFill={false}>
                    <DocumentSourceLogo
                        platform={platform}
                        size={16}
                        fallback={<FallbackIcon size={20} weight="regular" />}
                    />
                </IconWrapper>
            );
        }

        // Unpublished docs render as dashed variants. The `fill` weight on a dashed icon
        // collapses the dash pattern into a solid shape, so dashed icons stay `regular` even
        // when selected (the selected color is applied through IconWrapper).
        const Icon = pickTreeIcon({ hasChildren, isUnpublished });
        const iconWeight = isSelected && !isUnpublished ? 'fill' : 'regular';

        return (
            <IconWrapper className="tree-item-icon" $isSelected={isSelected} $useGradientFill={!isUnpublished}>
                <Icon size={20} weight={iconWeight} />
            </IconWrapper>
        );
    };

    return (
        <TreeItemContainer
            className="tree-item-container"
            data-testid={`document-tree-item-${urn}`}
            $isSelected={isSelected}
            onClick={handleItemClick}
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
        >
            <LeftContent>
                <ExpandZone $level={level} $expandable={hasChildren} onClick={handleExpandZoneClick}>
                    <IconSlot>{renderIcon()}</IconSlot>
                </ExpandZone>

                <Title $isSelected={isSelected} title={title}>
                    {title}
                </Title>
            </LeftContent>

            {multiSelect && (
                <CheckboxSlot>
                    <Checkbox
                        isChecked={isSelected}
                        setIsChecked={() => onClick()}
                        dataTestId={`document-tree-checkbox-${urn}`}
                    />
                </CheckboxSlot>
            )}

            {!multiSelect && !hideActions && (isHovered || forceShowActions) && (
                <Actions className="tree-item-actions">
                    {!hideActionsMenu && (
                        <DocumentActionsMenu
                            documentUrn={urn}
                            currentParentUrn={parentUrn}
                            shouldNavigateOnDelete={isSelected}
                            onMenuVisibilityChange={setForceShowActions}
                        />
                    )}
                    {!hideCreate && (
                        <Tooltip title={t('documents.newDocumentTooltip')} placement="bottom" showArrow={false}>
                            <ActionButton
                                icon={{ icon: Plus, color: 'icon' }}
                                variant="text"
                                onClick={handleAddChildClick}
                            />
                        </Tooltip>
                    )}
                </Actions>
            )}
        </TreeItemContainer>
    );
};
