import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { FileDashed } from '@phosphor-icons/react/dist/csr/FileDashed';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import { FolderDashed } from '@phosphor-icons/react/dist/csr/FolderDashed';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
import { DocumentActionsMenu } from '@app/homeV2/layout/sidebar/documents/DocumentActionsMenu';
import Loading from '@app/shared/Loading';
import { Button, Tooltip } from '@src/alchemy-components';

import { DataPlatform } from '@types';

const TreeItemContainer = styled.div<{ $level: number; $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
    min-height: 38px;
    height: 38px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.15s ease;
    margin-bottom: 2px;
    margin-left: 2px;
    margin-right: 2px;

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
    flex: 1;
    min-width: 0;
    overflow: hidden;
`;

const IconSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
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

/** Resolves the Phosphor icon component for a tree row given its branch/published state. */
function pickTreeIcon({ hasChildren, isUnpublished }: { hasChildren: boolean; isUnpublished: boolean }) {
    if (hasChildren) return isUnpublished ? FolderDashed : Folder;
    return isUnpublished ? FileDashed : FileText;
}

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
            $level={level}
            $isSelected={isSelected}
            onClick={handleItemClick}
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
        >
            <LeftContent>
                <IconSlot>{renderIcon()}</IconSlot>

                <Title $isSelected={isSelected} title={title}>
                    {title}
                </Title>
            </LeftContent>

            {!hideActions && (isHovered || forceShowActions) && (
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
                                icon={{ icon: Plus, color: 'gray', colorLevel: 1800 }}
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
