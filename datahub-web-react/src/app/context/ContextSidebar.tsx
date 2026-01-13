import { LoadingOutlined, SearchOutlined } from '@ant-design/icons';
import { Button, SearchBar, Tooltip } from '@components';
import { Divider } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useContextDocumentsPermissions } from '@app/context/useContextDocumentsPermissions';
import { useCreateDocumentTreeMutation } from '@app/document/hooks/useDocumentTreeMutations';
import { useSearchDocuments } from '@app/document/hooks/useSearchDocuments';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { SearchResultItem } from '@app/homeV2/layout/sidebar/documents/SearchResultItem';
import ClickOutside from '@app/shared/ClickOutside';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

import { DocumentSourceType, DocumentState, EntityType } from '@types';

const SIDEBAR_TRANSITION_MS = 300;
export const SIDEBAR_COLLAPSED_WIDTH = 63;

const SidebarContainer = styled.div<{
    $width: number;
    $isCollapsed: boolean;
    $isShowNavBarRedesign?: boolean;
    $isEntityProfile?: boolean;
}>`
    flex-shrink: 0;
    max-height: 100%;
    width: ${(props) => (props.$isCollapsed ? `${SIDEBAR_COLLAPSED_WIDTH}px` : `${props.$width}px`)};
    transition: width ${SIDEBAR_TRANSITION_MS}ms ease-in-out;
    background-color: #ffffff;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        margin: ${props.$isEntityProfile ? '5px 0px 6px 5px' : '0px 4px 0px 0px'};
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
    `}
`;

const HeaderControls = styled.div<{ $isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.$isCollapsed ? 'center' : 'space-between')};
    padding: 12px;
    height: 50px;
    overflow: hidden;
`;

const SidebarTitle = styled.div`
    font-size: 16px;
    font-weight: bold;
    color: #374066;
    white-space: nowrap;
`;

const HeaderButtons = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

const StyledButton = styled(Button)`
    padding: 2px;
    svg {
        width: 20px;
        height: 20px;
    }
`;

const ThinDivider = styled(Divider)`
    margin: 0px;
    padding: 0px;
`;

const SearchWrapper = styled.div`
    flex-shrink: 0;
    position: relative;
`;

const SearchInputWrapper = styled.div`
    padding: 12px;
`;

const SearchIcon = styled(SearchOutlined)`
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
    padding: 16px;
    width: 100%;
    font-size: 20px;
    cursor: pointer;

    &:hover {
        color: ${REDESIGN_COLORS.LINK_HOVER_BLUE};
    }
`;

const ResultsWrapper = styled.div`
    background-color: white;
    border-radius: 5px;
    box-shadow:
        0 3px 6px -4px rgb(0 0 0 / 12%),
        0 6px 16px 0 rgb(0 0 0 / 8%),
        0 9px 28px 8px rgb(0 0 0 / 5%);
    padding: 8px;
    position: absolute;
    max-height: 300px;
    overflow: auto;
    width: calc(100% - 8px);
    left: 4px;
    top: 55px;
    z-index: 1;
`;

const LoadingWrapper = styled(ResultsWrapper)`
    display: flex;
    justify-content: center;
    padding: 16px 0;
    font-size: 16px;
`;

const EmptyState = styled.div`
    padding: 16px;
    text-align: center;
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
    font-size: 14px;
`;

const TreeContainer = styled.div`
    flex: 1;
    overflow-y: auto;
    overflow-x: hidden;
    padding: 8px;

    /* Custom scrollbar styling */
    &::-webkit-scrollbar {
        width: 6px;
    }

    &::-webkit-scrollbar-track {
        background: transparent;
    }

    &::-webkit-scrollbar-thumb {
        background: #a9adbd;
        border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb:hover {
        background: #81879f;
    }

    scrollbar-width: thin;
    scrollbar-color: #a9adbd transparent;
`;

type Props = {
    isEntityProfile?: boolean;
    isCollapsed?: boolean;
    onToggleCollapsed?: () => void;
    onExpandSidebar?: () => void;
};

export default function ContextSidebar({
    isEntityProfile,
    isCollapsed = false,
    onToggleCollapsed,
    onExpandSidebar,
}: Props) {
    const [creating, setCreating] = useState(false);
    const [searchInput, setSearchInput] = useState('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const [isSearchBarFocused, setIsSearchBarFocused] = useState(false);
    const { createDocument } = useCreateDocumentTreeMutation();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();

    const { canCreate: canCreateDocuments } = useContextDocumentsPermissions();

    const width = useSidebarWidth(0.2);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    // Debounce search query
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedQuery(searchInput);
        }, 300);
        return () => clearTimeout(timer);
    }, [searchInput]);

    // Search for documents
    const { documents: searchResults, loading: searchLoading } = useSearchDocuments({
        query: debouncedQuery || '*',
        states: [DocumentState.Published, DocumentState.Unpublished],
        count: 50,
        fetchPolicy: 'network-only',
        includeParentDocuments: true,
        sourceTypes: [DocumentSourceType.Native],
        skip: !debouncedQuery,
    });

    const isSearching = debouncedQuery.trim().length > 0;

    const handleCreateDocument = useCallback(
        async (parentDocumentUrn?: string) => {
            if (!canCreateDocuments) return;

            setCreating(true);
            try {
                const newUrn = await createDocument({
                    title: 'New Document',
                    parentDocument: parentDocumentUrn || null,
                });

                if (newUrn) {
                    const url = entityRegistry.getEntityUrl(EntityType.Document, newUrn);
                    history.push(url);
                }
            } finally {
                setCreating(false);
            }
        },
        [canCreateDocuments, createDocument, entityRegistry, history],
    );

    const handleDocumentClick = useCallback(
        (urn: string) => {
            const url = entityRegistry.getEntityUrl(EntityType.Document, urn);
            history.push(url);
            setIsSearchBarFocused(false);
            setSearchInput('');
        },
        [entityRegistry, history],
    );

    return (
        <SidebarContainer
            $width={width}
            $isCollapsed={isCollapsed}
            data-testid="context-documents-sidebar"
            $isShowNavBarRedesign={isShowNavBarRedesign}
            $isEntityProfile={isEntityProfile}
        >
            <HeaderControls $isCollapsed={isCollapsed}>
                {!isCollapsed && <SidebarTitle>Documents</SidebarTitle>}
                <HeaderButtons>
                    {!isCollapsed && (
                        <Tooltip
                            title={
                                canCreateDocuments
                                    ? 'Create Document'
                                    : 'Reach out to your DataHub admin to create documents.'
                            }
                            placement="bottom"
                            showArrow={false}
                        >
                            <span style={{ display: 'inline-block' }}>
                                <StyledButton
                                    variant="filled"
                                    color="primary"
                                    isCircle
                                    icon={{ icon: 'Plus', source: 'phosphor' }}
                                    onClick={() => handleCreateDocument()}
                                    disabled={!canCreateDocuments || creating}
                                    data-testid="create-document-button"
                                />
                            </span>
                        </Tooltip>
                    )}
                    <Button
                        variant="text"
                        color="gray"
                        size="lg"
                        isCircle
                        icon={{ icon: isCollapsed ? 'ArrowLineRight' : 'ArrowLineLeft', source: 'phosphor' }}
                        isActive={!isCollapsed}
                        onClick={onToggleCollapsed}
                        data-testid="context-sidebar-collapse-button"
                    />
                </HeaderButtons>
            </HeaderControls>
            <ThinDivider />

            {/* Search Section */}
            <SearchWrapper>
                {isCollapsed ? (
                    <SearchIcon onClick={onExpandSidebar} data-testid="context-sidebar-search-icon" />
                ) : (
                    <ClickOutside onClickOutside={() => setIsSearchBarFocused(false)}>
                        <SearchInputWrapper>
                            <SearchBar
                                placeholder="Search documents"
                                value={searchInput}
                                onChange={setSearchInput}
                                onFocus={() => setIsSearchBarFocused(true)}
                                data-testid="context-sidebar-search-input"
                            />
                        </SearchInputWrapper>
                        {searchLoading && isSearchBarFocused && isSearching && (
                            <LoadingWrapper>
                                <LoadingOutlined />
                            </LoadingWrapper>
                        )}
                        {!searchLoading && isSearchBarFocused && isSearching && searchResults.length === 0 && (
                            <ResultsWrapper>
                                <EmptyState>No results found</EmptyState>
                            </ResultsWrapper>
                        )}
                        {!searchLoading && isSearchBarFocused && isSearching && searchResults.length > 0 && (
                            <ResultsWrapper data-testid="context-sidebar-search-results">
                                {searchResults.map((doc) => {
                                    // Build breadcrumb from parentDocuments array
                                    let breadcrumb: string | null = null;
                                    if (doc.parentDocuments?.documents && doc.parentDocuments.documents.length > 0) {
                                        const parents = [...doc.parentDocuments.documents].reverse();
                                        breadcrumb = parents
                                            .map((parent) => parent.info?.title || 'Untitled')
                                            .join(' > ');
                                    }

                                    return (
                                        <SearchResultItem
                                            key={doc.urn}
                                            doc={doc}
                                            level={0}
                                            isSelected={false}
                                            hasChildren={false}
                                            isExpanded={false}
                                            isLoading={false}
                                            breadcrumb={breadcrumb}
                                            onSelect={() => handleDocumentClick(doc.urn)}
                                            onToggleExpand={() => {}}
                                        />
                                    );
                                })}
                            </ResultsWrapper>
                        )}
                    </ClickOutside>
                )}
            </SearchWrapper>

            {!isCollapsed && (
                <>
                    <ThinDivider />
                    <TreeContainer>
                        <DocumentTree onCreateChild={(parentUrn) => handleCreateDocument(parentUrn || undefined)} />
                    </TreeContainer>
                </>
            )}
        </SidebarContainer>
    );
}
