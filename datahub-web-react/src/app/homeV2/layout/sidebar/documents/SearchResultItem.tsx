import { CaretDown, CaretRight, FileText, Folder } from '@phosphor-icons/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { DocumentChild } from '@app/documentV2/hooks/useDocumentChildren';
import Loading from '@app/shared/Loading';
import { colors } from '@src/alchemy-components/theme';

import { Document } from '@types';

const SearchResultItemContainer = styled.div<{ $isSelected: boolean; $level: number }>`
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
    border-radius: 6px;
    cursor: pointer;
    margin-bottom: 2px;
    transition: background-color 0.15s ease;
    display: flex;
    align-items: center;
    gap: 4px;

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
            background-color: ${colors.gray[100]};
        }
    `}
`;

const SearchResultContent = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 0;
    margin-left: 8px; /* Add spacing between icon and text */
`;

const SearchResultTitle = styled.div`
    font-size: 14px;
    line-height: 20px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: ${colors.gray[1700]};
`;

const SearchResultBreadcrumb = styled.div`
    font-size: 12px;
    color: ${colors.gray[500]};
    line-height: 16px;
    margin-top: 2px;
`;

const IconWrapper = styled.div`
    display: flex;
    align-items: center;
    color: ${colors.gray[600]};
    flex-shrink: 0;
`;

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
    color: ${colors.gray[600]};
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
                    <IconWrapper>
                        {hasChildren ? (
                            <Folder size={16} weight={isSelected ? 'fill' : 'regular'} />
                        ) : (
                            <FileText size={16} weight={isSelected ? 'fill' : 'regular'} />
                        )}
                    </IconWrapper>
                )}
                <SearchResultContent>
                    <SearchResultTitle>{title}</SearchResultTitle>
                    {level === 0 && breadcrumb && <SearchResultBreadcrumb>{breadcrumb}</SearchResultBreadcrumb>}
                </SearchResultContent>
            </SearchResultItemContainer>
            {isExpanded && children}
        </>
    );
};
