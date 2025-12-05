import { CaretDown, CaretRight, FileText, Folder } from '@phosphor-icons/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { DocumentChild } from '@app/document/hooks/useDocumentChildren';
import Loading from '@app/shared/Loading';
import { colors } from '@src/alchemy-components/theme';
import { getColor } from '@src/alchemy-components/theme/utils';

import { Document } from '@types';

const SearchResultItemContainer = styled.div<{ $isSelected: boolean; $level: number }>`
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
        props.$isSelected
            ? `
        background: linear-gradient(
            180deg,
            rgba(83, 63, 209, 0.04) -3.99%,
            rgba(112, 94, 228, 0.04) 53.04%,
            rgba(112, 94, 228, 0.04) 100%
        );
        box-shadow: 0px 0px 0px 1px rgba(108, 71, 255, 0.08);
    `
            : `
        &:hover {
            background-color: ${colors.gray[1500]};
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

const SearchResultContent = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 0;
`;

const SearchResultTitle = styled.span<{ $isSelected: boolean }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 14px;
    line-height: 20px;
    color: ${colors.gray[1700]};

    ${(props) =>
        props.$isSelected &&
        `
        background: linear-gradient(${getColor('primary', 300, props.theme)} 1%, ${getColor('primary', 500, props.theme)} 99%);
        background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 600;
    `}
`;

const SearchResultBreadcrumb = styled.div`
    font-size: 12px;
    color: ${colors.gray[500]};
    line-height: 16px;
    margin-top: 2px;
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

const ExpandButton = styled.button`
    display: flex;
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

interface SearchResultItemProps {
    /** Document or child document to render */
    doc: Document | DocumentChild;
    /** Nesting level for indentation */
    level: number;
    /** Whether this item is currently selected */
    isSelected: boolean;
    /** Whether this document has children */
    hasChildren: boolean;
    /** Whether this document is currently expanded */
    isExpanded: boolean;
    /** Whether children are currently loading */
    isLoading: boolean;
    /** Breadcrumb text to show (parent path) */
    breadcrumb?: string | null;
    /** Children nodes to render when expanded */
    children?: React.ReactNode;
    /** Callback when item is clicked */
    onSelect: () => void;
    /** Callback when expand button is clicked */
    onToggleExpand: () => void;
}

/**
 * Component for rendering a single search result item in the move document popover.
 * Supports:
 * - Nested display with indentation
 * - Expand/collapse for documents with children
 * - Hover state management
 * - Icon display (folder for parents, file for children)
 * - Breadcrumb display for context
 *
 * Extracted from MoveDocumentPopover to improve reusability and performance.
 */
export const SearchResultItem: React.FC<SearchResultItemProps> = ({
    doc,
    level,
    isSelected,
    hasChildren,
    isExpanded,
    isLoading,
    breadcrumb,
    children,
    onSelect,
    onToggleExpand,
}) => {
    const [isHovered, setIsHovered] = useState(false);

    // Determine document title
    const isDocument = 'info' in doc;
    const title = isDocument ? doc.info?.title || 'Untitled' : (doc as DocumentChild).title;

    // Match DocumentTreeItem behavior: show expand button on hover or when expanded
    const showExpandButton = hasChildren && (isExpanded || isHovered);
    const showIcon = !showExpandButton;

    return (
        <>
            <SearchResultItemContainer
                $isSelected={isSelected}
                $level={level}
                onClick={onSelect}
                onMouseEnter={() => setIsHovered(true)}
                onMouseLeave={() => setIsHovered(false)}
            >
                <LeftContent>
                    {showExpandButton && (
                        <ExpandButton
                            onClick={(e) => {
                                e.stopPropagation();
                                onToggleExpand();
                            }}
                        >
                            {isLoading && <Loading height={16} marginTop={0} alignItems="center" />}
                            {!isLoading && isExpanded && <CaretDown size={16} weight="bold" />}
                            {!isLoading && !isExpanded && <CaretRight size={16} weight="bold" />}
                        </ExpandButton>
                    )}
                    {showIcon && (
                        <IconWrapper $isSelected={isSelected}>
                            {hasChildren ? (
                                <Folder size={16} weight={isSelected ? 'fill' : 'regular'} />
                            ) : (
                                <FileText size={16} weight={isSelected ? 'fill' : 'regular'} />
                            )}
                        </IconWrapper>
                    )}
                    <SearchResultContent>
                        <SearchResultTitle
                            $isSelected={isSelected}
                            data-testid="move-popover-search-result-title"
                            title={title}
                        >
                            {title}
                        </SearchResultTitle>
                        {level === 0 && breadcrumb && <SearchResultBreadcrumb>{breadcrumb}</SearchResultBreadcrumb>}
                    </SearchResultContent>
                </LeftContent>
            </SearchResultItemContainer>
            {isExpanded && children}
        </>
    );
};
