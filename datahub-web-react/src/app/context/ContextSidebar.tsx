import { Avatar, Button, Loader, SearchBar, Tooltip } from '@components';
import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Divider } from 'antd';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { AvatarType } from '@components/components/AvatarStack/types';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import { useContextDocumentsPermissions } from '@app/context/useContextDocumentsPermissions';
import { useDocumentFilters } from '@app/document/DocumentFiltersContext';
import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
import { useDocumentTree } from '@app/document/DocumentTreeContext';
import { useCreateDocumentTreeMutation } from '@app/document/hooks/useDocumentTreeMutations';
import { useSearchDocuments } from '@app/document/hooks/useSearchDocuments';
import {
    DEFAULT_STATUS_FILTER,
    DocumentStatusFilter,
    getAvailablePlatforms,
    getDistinctCreators,
} from '@app/document/utils/documentTreeFilters';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { SearchResultItem } from '@app/homeV2/layout/sidebar/documents/SearchResultItem';
import ClickOutside from '@app/shared/ClickOutside';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

import { DocumentSourceType, DocumentState, EntityType } from '@types';

const SIDEBAR_TRANSITION_MS = 300;
export const SIDEBAR_COLLAPSED_WIDTH = 63;

// URN prefix used to identify AI-agent actors. Documents authored by an agent
// are filtered out of the Author multi-select in OSS — agents aren't a first-
// class concept here, so they would render as orphan "human" rows.
const AI_AGENT_URN_PREFIX = 'urn:li:aiAgent:';

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
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
 margin: ${props.$isEntityProfile ? '5px 0px 6px 5px' : '5px 0px 5px 5px'};
 box-shadow: ${props.theme.colors.shadowSm};
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
    color: ${(props) => props.theme.colors.text};
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

// Two SimpleSelects packed to the start of the row directly under the search bar.
// Padding matches SearchInputWrapper so the row aligns with the search field above.
// Each select uses `width="fit-content"` so labels/values size to content rather than
// stretching to fill the row.
const FiltersRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    padding: 0 12px 12px 12px;
`;

const SearchIconButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    padding: 16px 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: ${(props) => props.theme.colors.icon};

    &:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

const ResultsWrapper = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 5px;
    box-shadow: ${(props) => props.theme.colors.shadowLg};
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
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 14px;
`;

// Row used to pair a platform logo with its label inside the Source filter's
// multi-select dropdown. SimpleSelect renders option icons natively only in
// single-select mode, so the multi-select Source filter has to supply its own
// option renderer (`renderCustomOptionText`) — this is the layout it returns.
const SourceOptionRow = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
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
        background: ${(props) => props.theme.colors.textTertiary};
        border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb:hover {
        background: ${(props) => props.theme.colors.textTertiary};
    }

    scrollbar-width: thin;
    scrollbar-color: ${(props) => props.theme.colors.textTertiary} transparent;
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
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.actions');
    const [creating, setCreating] = useState(false);
    const [searchInput, setSearchInput] = useState('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const [isSearchBarFocused, setIsSearchBarFocused] = useState(false);
    const {
        status: statusFilter,
        selectedAuthorUrns,
        selectedPlatformUrns,
        setStatus: setStatusFilter,
        setSelectedAuthorUrns,
        setSelectedPlatformUrns,
    } = useDocumentFilters();
    const { createDocument } = useCreateDocumentTreeMutation();
    const { expandNode, getNode, nodes } = useDocumentTree();
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

    const expandAncestors = useCallback(
        (parentUrn?: string | null) => {
            let current = parentUrn || null;
            while (current) {
                expandNode(current);
                current = getNode(current)?.parentUrn || null;
            }
        },
        [expandNode, getNode],
    );

    // Status filter: static enum-like list, mutually exclusive.
    const statusOptions = useMemo(
        () => [
            { value: 'all', label: t('context.statusFilter.all') },
            { value: 'published', label: t('context.statusFilter.published') },
            { value: 'unpublished', label: t('context.statusFilter.unpublished') },
        ],
        [t],
    );

    // Author filter: fully dynamic — one row per distinct human creator present
    // in the currently-loaded tree. Agent actors (`urn:li:aiAgent:*`) are
    // filtered out: agents aren't a first-class concept in OSS, so showing them
    // here would surface URNs as orphan rows.
    const nodeList = useMemo(() => Array.from(nodes.values()), [nodes]);
    const distinctCreators = useMemo(
        () => getDistinctCreators(nodeList).filter((c) => !c.urn.startsWith(AI_AGENT_URN_PREFIX)),
        [nodeList],
    );
    const authorOptions = useMemo(
        () =>
            distinctCreators.map((creator) => ({
                value: creator.urn,
                label: creator.displayName,
                creator,
            })),
        [distinctCreators],
    );

    // Source filter: fully dynamic — one row per platform present in the
    // currently-loaded tree. Native DataHub docs carry the DataHub platform
    // already, so there's no separate "native" sentinel row to merge in.
    const availablePlatforms = useMemo(() => getAvailablePlatforms(nodeList), [nodeList]);
    const sourceOptions = useMemo(
        () =>
            availablePlatforms.map((platform) => ({
                value: platform.urn,
                label: entityRegistry.getDisplayName(EntityType.DataPlatform, platform),
                icon: <DocumentSourceLogo platform={platform} size={16} />,
            })),
        [availablePlatforms, entityRegistry],
    );

    const filterSelection = useMemo(
        () => ({
            status: statusFilter,
            selectedAuthorUrns: selectedAuthorUrns.length > 0 ? selectedAuthorUrns : null,
            selectedPlatformUrns: selectedPlatformUrns.length > 0 ? selectedPlatformUrns : null,
        }),
        [statusFilter, selectedAuthorUrns, selectedPlatformUrns],
    );

    const handleCreateDocument = useCallback(
        async (parentDocumentUrn?: string) => {
            if (!canCreateDocuments) return;

            setCreating(true);
            try {
                // Ensure ancestors are expanded so the new doc is visible
                expandAncestors(parentDocumentUrn || null);

                const newUrn = await createDocument({
                    /* untranslated-text -- default new-document title persisted as backend data, not UI chrome */
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
        [canCreateDocuments, createDocument, entityRegistry, expandAncestors, history],
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
                {!isCollapsed && <SidebarTitle>{t('context.documentsSidebarTitle')}</SidebarTitle>}
                <HeaderButtons>
                    {!isCollapsed && (
                        <Tooltip
                            title={
                                canCreateDocuments
                                    ? t('context.createDocumentTooltip')
                                    : t('context.noCreateDocumentPermissionTooltip')
                            }
                            placement="bottom"
                            showArrow={false}
                        >
                            <span style={{ display: 'inline-block' }}>
                                <StyledButton
                                    variant="filled"
                                    color="primary"
                                    isCircle
                                    icon={{ icon: Plus }}
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
                        icon={{ icon: isCollapsed ? ArrowLineRight : ArrowLineLeft }}
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
                    <SearchIconButton
                        onClick={onExpandSidebar}
                        data-testid="context-sidebar-search-icon"
                        aria-label={t('context.searchDocumentsAriaLabel')}
                    >
                        <MagnifyingGlass size={20} weight="regular" />
                    </SearchIconButton>
                ) : (
                    <ClickOutside onClickOutside={() => setIsSearchBarFocused(false)}>
                        <SearchInputWrapper>
                            <SearchBar
                                placeholder={t('context.searchDocumentsPlaceholder')}
                                value={searchInput}
                                onChange={setSearchInput}
                                onFocus={() => setIsSearchBarFocused(true)}
                                data-testid="context-sidebar-search-input"
                            />
                        </SearchInputWrapper>
                        {searchLoading && isSearchBarFocused && isSearching && (
                            <LoadingWrapper>
                                <Loader size="md" />
                            </LoadingWrapper>
                        )}
                        {!searchLoading && isSearchBarFocused && isSearching && searchResults.length === 0 && (
                            <ResultsWrapper>
                                <EmptyState>{tc('noResults')}</EmptyState>
                            </ResultsWrapper>
                        )}
                        {!searchLoading && isSearchBarFocused && isSearching && searchResults.length > 0 && (
                            <ResultsWrapper data-testid="context-sidebar-search-results">
                                {searchResults.map((doc) => {
                                    // Build breadcrumb from parentDocuments array
                                    let breadcrumb: string | null = null;
                                    /* eslint-disable i18next/no-literal-string -- (untranslated-text) 'Untitled' is a fallback default for a missing document title (data, not UI chrome); ' > ' is a punctuation separator */
                                    if (doc.parentDocuments?.documents && doc.parentDocuments.documents.length > 0) {
                                        const parents = [...doc.parentDocuments.documents].reverse();
                                        breadcrumb = parents
                                            .map((parent) => parent.info?.title || 'Untitled')
                                            .join(' > ');
                                    }
                                    /* eslint-enable i18next/no-literal-string */

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
                <FiltersRow>
                    <SimpleSelect
                        size="sm"
                        width="fit-content"
                        showClear={false}
                        placeholder={t('context.statusFilter.placeholder')}
                        // Use the 'default' label variant (single-select with no
                        // pill / no static "Status:" prefix) so the trigger reads
                        // as the live value — "All", "Published", or "Unpublished".
                        // The Author and Source multi-selects keep the labeled
                        // variant because their selection isn't a single word.
                        selectLabelProps={{ variant: 'default' }}
                        options={statusOptions}
                        values={[statusFilter]}
                        onUpdate={(values) =>
                            setStatusFilter((values[0] as DocumentStatusFilter) || DEFAULT_STATUS_FILTER)
                        }
                        dataTestId="context-sidebar-status-filter"
                    />
                    <SimpleSelect
                        size="sm"
                        width="fit-content"
                        isMultiSelect
                        isDisabled={authorOptions.length === 0}
                        placeholder={t('context.authorFilter.placeholder')}
                        selectLabelProps={{ variant: 'labeled', label: t('context.authorFilter.label') }}
                        options={authorOptions}
                        values={selectedAuthorUrns}
                        onUpdate={setSelectedAuthorUrns}
                        renderCustomOptionText={(option) => {
                            const { creator } = option as (typeof authorOptions)[number];
                            return (
                                <Avatar
                                    name={creator.displayName}
                                    imageUrl={creator.pictureLink ?? undefined}
                                    type={creator.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user}
                                    showInPill
                                    size="sm"
                                />
                            );
                        }}
                        dataTestId="context-sidebar-author-filter"
                    />
                    <SimpleSelect
                        size="sm"
                        width="fit-content"
                        isMultiSelect
                        isDisabled={sourceOptions.length === 0}
                        placeholder={t('context.sourceFilter.placeholder')}
                        selectLabelProps={{ variant: 'labeled', label: t('context.sourceFilter.label') }}
                        options={sourceOptions}
                        values={selectedPlatformUrns}
                        onUpdate={setSelectedPlatformUrns}
                        renderCustomOptionText={(option) => (
                            <SourceOptionRow>
                                {option.icon}
                                <span>{option.label}</span>
                            </SourceOptionRow>
                        )}
                        dataTestId="context-sidebar-source-filter"
                    />
                </FiltersRow>
            )}

            {!isCollapsed && (
                <>
                    <ThinDivider />
                    <TreeContainer>
                        <DocumentTree
                            onCreateChild={(parentUrn) => handleCreateDocument(parentUrn || undefined)}
                            filterSelection={filterSelection}
                        />
                    </TreeContainer>
                </>
            )}
        </SidebarContainer>
    );
}
