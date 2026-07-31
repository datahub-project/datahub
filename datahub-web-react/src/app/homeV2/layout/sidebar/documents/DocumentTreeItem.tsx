import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
import { pickTreeIcon } from '@app/document/utils/documentUtils';
import { DocumentActionsMenu } from '@app/homeV2/layout/sidebar/documents/DocumentActionsMenu';
import Loading from '@app/shared/Loading';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { Button, Checkbox, Tooltip } from '@src/alchemy-components';

import { DataPlatform } from '@types';

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

const ActionsWrap = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

interface DocumentTreeItemProps {
    urn: string;
    title: string;
    level: number;
    hasChildren: boolean;
    isExpanded: boolean;
    isSelected: boolean;
    childCount?: number;
    isLoading?: boolean;
    isUnpublished?: boolean;
    isExternal?: boolean;
    platform?: DataPlatform | null;
    onToggleExpand: () => void;
    onClick: () => void;
    onCreateChild: (parentUrn: string) => void;
    hideActions?: boolean;
    hideActionsMenu?: boolean;
    hideCreate?: boolean;
    parentUrn?: string | null;
    multiSelect?: boolean;
}

export const DocumentTreeItem: React.FC<DocumentTreeItemProps> = ({
    urn,
    title,
    level,
    hasChildren,
    isExpanded,
    isSelected,
    childCount,
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
    const [isHovered, setIsHovered] = useState(false);
    const [forceShowActions, setForceShowActions] = useState(false);
    const rowRef = useRef<HTMLDivElement>(null);
    const didScrollForSelectionRef = useRef(false);

    // Deep links mount the selected row after ancestors expand — scroll once (auto).
    useEffect(() => {
        if (!isSelected || multiSelect) {
            didScrollForSelectionRef.current = false;
            return;
        }
        if (didScrollForSelectionRef.current) return;
        didScrollForSelectionRef.current = true;
        rowRef.current?.scrollIntoView({ block: 'nearest', behavior: 'auto' });
    }, [isSelected, multiSelect, urn]);

    const handleAddChildClick = (e: React.MouseEvent) => {
        e.stopPropagation();
        onCreateChild(urn);
    };

    const handleItemClick = (e?: React.MouseEvent) => {
        if (e && (e.target as HTMLElement).closest('.tree-item-actions')) {
            return;
        }
        onClick();
    };

    const showActions = !multiSelect && !hideActions && (isHovered || forceShowActions);
    // Keep the count mounted while actions show so the right edge doesn't jump on hover.
    const showCount = !multiSelect && hasChildren && !isExpanded && childCount != null && childCount > 0;

    const restingIcon = (() => {
        if (isLoading) {
            return <Loading height={16} marginTop={0} alignItems="center" />;
        }
        if (isExternal && platform) {
            const FallbackIcon = pickTreeIcon({ hasChildren, isUnpublished: false });
            return (
                <IconWrapper className="tree-item-icon" $isSelected={false} $useGradientFill={false}>
                    <DocumentSourceLogo
                        platform={platform}
                        size={16}
                        fallback={<FallbackIcon size={TREE_ROW_ENTITY_ICON_SIZE} weight="regular" />}
                    />
                </IconWrapper>
            );
        }
        const Icon = pickTreeIcon({ hasChildren, isUnpublished });
        const iconWeight = isSelected && !isUnpublished ? 'fill' : 'regular';
        return (
            <IconWrapper className="tree-item-icon" $isSelected={isSelected} $useGradientFill={!isUnpublished}>
                <Icon size={TREE_ROW_ENTITY_ICON_SIZE} weight={iconWeight} />
            </IconWrapper>
        );
    })();

    let trailing: React.ReactNode;
    if (multiSelect) {
        trailing = (
            <CheckboxSlot>
                <Checkbox
                    isChecked={isSelected}
                    setIsChecked={() => onClick()}
                    dataTestId={`document-tree-checkbox-${urn}`}
                />
            </CheckboxSlot>
        );
    } else if (showActions) {
        trailing = (
            <ActionsWrap className="tree-item-actions">
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
            </ActionsWrap>
        );
    }

    return (
        <HierarchicalBrowseTreeRow
            ref={rowRef}
            className="tree-item-container"
            data-testid={`document-tree-item-${urn}`}
            level={level}
            isSelected={isSelected}
            hasChildren={hasChildren}
            isExpanded={isExpanded}
            count={showCount ? childCount : undefined}
            countReveal="hover"
            icon={restingIcon}
            label={title}
            labelTitle={title}
            trailing={trailing}
            onSelect={() => handleItemClick()}
            onToggleExpand={onToggleExpand}
            isLoadingChildren={!!isLoading}
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
        />
    );
};
