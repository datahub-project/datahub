import { CaretDown, CaretRight, FileText, Folder } from '@phosphor-icons/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { DocumentActionsMenu } from '@app/homeV2/layout/sidebar/documents/DocumentActionsMenu';
import Loading from '@app/shared/Loading';
import { Button, Tooltip } from '@src/alchemy-components';
import { colors } from '@src/alchemy-components/theme';
import { getColor } from '@src/alchemy-components/theme/utils';

const TreeItemContainer = styled.div<{ $level: number; $isSelected: boolean }>`
    position: relative;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
    min-height: 38px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.15s ease;
    margin-bottom: 2px;
    margin-left: 2px;
    margin-right: 2px;

    ${(props) =>
        props.$isSelected &&
        `
        background: linear-gradient(
            180deg,
            rgba(83, 63, 209, 0.04) -3.99%,
            rgba(112, 94, 228, 0.04) 53.04%,
            rgba(112, 94, 228, 0.04) 100%
        );
        box-shadow: 0px 0px 0px 1px rgba(108, 71, 255, 0.08);
    `}

    ${(props) =>
        !props.$isSelected &&
        `
        &:hover {
            background-color: ${colors.gray[100]};
        }
    `}

    &:hover .tree-item-actions {
        opacity: 1;
    }
`;

const LeftContent = styled.div`
    display: flex;
    align-items: center;
    flex: 1;
    min-width: 0;
    overflow: hidden;
`;

const ExpandButton = styled.button<{ $hasChildren: boolean; $isExpanded: boolean }>`
    display: ${(props) => (props.$hasChildren && props.$isExpanded ? 'flex' : 'none')};
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    margin-right: 4px;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: inherit;
    flex-shrink: 0;

    &:hover {
        opacity: 0.7;
    }
`;

const IconWrapper = styled.div<{ $isSelected: boolean }>`
    display: flex;
    align-items: center;
    margin-right: 8px;
    flex-shrink: 0;

    && svg {
        ${(props) =>
            props.$isSelected
                ? `fill: url(#menu-item-selected-gradient) ${props.theme.styles?.['primary-color'] || '#6C47FF'};`
                : 'color: #8088a3;'}
    }
`;

const Title = styled.span<{ $isSelected: boolean }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 14px;
    line-height: 20px;

    ${(props) =>
        props.$isSelected &&
        `
        background: linear-gradient(${getColor('primary', 300, props.theme)} 1%, ${getColor('primary', 500, props.theme)} 99%);
        background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 600;
    `}
`;

const Actions = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    opacity: 0;
    transition: opacity 0.15s ease;
    margin-left: 8px;
`;

const ActionButton = styled(Button)`
    &:hover {
        background-color: ${colors.gray[200]};
    }
`;

interface DocumentTreeItemProps {
    urn: string;
    title: string;
    level: number;
    hasChildren: boolean;
    isExpanded: boolean;
    isSelected: boolean;
    isLoading?: boolean;
    onToggleExpand: () => void;
    onClick: () => void;
    onCreateChild: (parentUrn: string) => void;
    hideActions?: boolean;
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
    onToggleExpand,
    onClick,
    onCreateChild,
    hideActions = false,
    parentUrn,
}) => {
    const [isHovered, setIsHovered] = useState(false);

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
    const showIcon = !showExpandButton;

    return (
        <TreeItemContainer
            className="tree-item-container"
            $level={level}
            $isSelected={isSelected}
            onClick={handleItemClick}
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
        >
            <LeftContent>
                {showExpandButton && (
                    <ExpandButton
                        className="tree-item-expand-button"
                        $hasChildren={hasChildren}
                        $isExpanded={isExpanded || isHovered}
                        onClick={handleExpandClick}
                        aria-label={isExpanded ? 'Collapse' : 'Expand'}
                    >
                        {isLoading && <Loading height={16} marginTop={0} alignItems="center" />}
                        {!isLoading && isExpanded && <CaretDown size={16} weight="bold" />}
                        {!isLoading && !isExpanded && <CaretRight size={16} weight="bold" />}
                    </ExpandButton>
                )}

                {showIcon && (
                    <IconWrapper className="tree-item-icon" $isSelected={isSelected}>
                        {hasChildren ? (
                            <Folder size={16} weight={isSelected ? 'fill' : 'regular'} />
                        ) : (
                            <FileText size={16} weight={isSelected ? 'fill' : 'regular'} />
                        )}
                    </IconWrapper>
                )}

                <Title $isSelected={isSelected}>{title}</Title>
            </LeftContent>

            {!hideActions && (
                <Actions className="tree-item-actions">
                    <DocumentActionsMenu documentUrn={urn} currentParentUrn={parentUrn} />
                    <Tooltip title="New document" placement="top" showArrow={false}>
                        <ActionButton
                            icon={{ icon: 'Plus', source: 'phosphor' }}
                            variant="text"
                            onClick={handleAddChildClick}
                        />
                    </Tooltip>
                </Actions>
            )}
        </TreeItemContainer>
    );
};
