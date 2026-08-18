import { Avatar, SearchBar, Tooltip } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useHistory, useLocation } from 'react-router-dom';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import DocumentSidebarSearchFilters from '@app/context/DocumentSidebarSearchFilters';
import DocumentSidebarSearchResults from '@app/context/DocumentSidebarSearchResults';
import ImportDocumentsButton from '@app/context/import/ImportDocumentsButton';
import { useDocumentImportSuccess } from '@app/context/import/hooks/useDocumentImportSuccess';
import { useContextDocumentsPermissions } from '@app/context/useContextDocumentsPermissions';
import { useUserContext } from '@app/context/useUserContext';
import { useDocumentFilters } from '@app/document/DocumentFiltersContext';
import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
import { useDocumentTree } from '@app/document/DocumentTreeContext';
import useDocumentSidebarFacetOptions, {
    isDataPlatformEntity,
} from '@app/document/hooks/useDocumentSidebarFacetOptions';
import useDocumentSidebarSearch from '@app/document/hooks/useDocumentSidebarSearch';
import { useCreateDocumentTreeMutation } from '@app/document/hooks/useDocumentTreeMutations';
import { useLoadDocumentTree } from '@app/document/hooks/useLoadDocumentTree';
import {
    SECONDARY_BROWSE_FILTERS,
    SecondaryBrowseFilter,
    isDocumentSidebarSearchActive,
    isSecondaryBrowseFilter,
    nextPromotedBrowseFilters,
} from '@app/document/utils/documentSidebarMode';
import {
    DEFAULT_DOCUMENT_SIDEBAR_SORT,
    DOCUMENT_SIDEBAR_SORT,
    DocumentSidebarSortValue,
} from '@app/document/utils/documentSidebarSort';
import { DEFAULT_STATUS_FILTER, DocumentStatusFilter } from '@app/document/utils/documentTreeFilters';
import { decodeUrn } from '@app/entityV2/shared/utils';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import { SidebarCreateButton } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import SidebarAddFilter from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarAddFilter';
import SidebarSortSelect from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarSortSelect';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

// Row used to pair a platform logo with its label inside the Source filter's
// multi-select dropdown. SimpleSelect renders option icons natively only in
// single-select mode, so the multi-select Source filter has to supply its own
// option renderer (`renderCustomOptionText`) — this is the layout it returns.
const SourceOptionRow = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
`;

type Props = {
    isEntityProfile?: boolean;
    isCollapsed?: boolean;
    onToggleCollapsed?: () => void;
    onExpandSidebar?: () => void;
    onWidthChange?: (width: number) => void;
};

export default function ContextSidebar({
    isEntityProfile,
    isCollapsed = false,
    onToggleCollapsed,
    onExpandSidebar,
    onWidthChange,
}: Props) {
    const { t } = useTranslation('misc');
    const [creating, setCreating] = useState(false);
    const [searchInput, setSearchInput] = useState('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const [selectedDomainUrns, setSelectedDomainUrns] = useState<string[]>([]);
    const [selectedTagUrns, setSelectedTagUrns] = useState<string[]>([]);
    const [selectedTermUrns, setSelectedTermUrns] = useState<string[]>([]);
    const [selectedTypeNames, setSelectedTypeNames] = useState<string[]>([]);
    const [sortSelection, setSortSelection] = useState<DocumentSidebarSortValue>(DEFAULT_DOCUMENT_SIDEBAR_SORT);
    // After sort, keep list at top — selected-row scrollIntoView would jump to the open doc.
    const [suppressSelectionScroll, setSuppressSelectionScroll] = useState(false);
    const isFirstSortEffectRef = useRef(true);
    // Notion-style: Tag / Status / Author / Source start behind "+ Filter" until promoted.
    const [promotedBrowseFilters, setPromotedBrowseFilters] = useState<Set<SecondaryBrowseFilter>>(new Set());
    // One-shot: open the dropdown for a filter just chosen from "+ Filter".
    const [filterToAutoOpen, setFilterToAutoOpen] = useState<SecondaryBrowseFilter | null>(null);
    // Bumps when a filter is added so SimpleSelect remounts with a fresh defaultOpen.
    const [autoOpenNonce, setAutoOpenNonce] = useState(0);
    const {
        status: statusFilter,
        selectedAuthorUrns,
        selectedPlatformUrns,
        setStatus: setStatusFilter,
        setSelectedAuthorUrns,
        setSelectedPlatformUrns,
    } = useDocumentFilters();
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;
    const { createDocument } = useCreateDocumentTreeMutation();
    const { expandNode, getNode, setExpandedUrns } = useDocumentTree();
    const { loadChildren } = useLoadDocumentTree(sortSelection, { paginateRoots: false });
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

    // Sort is server-side: remounting DocumentTree reloads roots from page 1.
    // Drop expansion that would point at wiped child lists, suppress selection
    // scroll, and pin the browse list at the top (same as search).
    useEffect(() => {
        if (isFirstSortEffectRef.current) {
            isFirstSortEffectRef.current = false;
            return;
        }
        setExpandedUrns(new Set());
        setSuppressSelectionScroll(true);
        const treeScroll = document.querySelector('[data-testid="hierarchical-browse-tree-scroll"]');
        if (treeScroll instanceof HTMLElement) {
            treeScroll.scrollTop = 0;
        }
    }, [sortSelection, setExpandedUrns]);

    const importParentDocumentUrn = useMemo(() => {
        if (!isEntityProfile) {
            return undefined;
        }
        const documentPath = `/${entityRegistry.getPathName(EntityType.Document)}/:urn`;
        const match = matchPath<{ urn: string }>(location.pathname, { path: documentPath });
        if (!match?.params.urn) {
            return undefined;
        }
        return decodeUrn(match.params.urn);
    }, [entityRegistry, isEntityProfile, location.pathname]);

    const { canCreate: canCreateDocuments } = useContextDocumentsPermissions();

    useDebounce(() => setDebouncedQuery(searchInput), 300, [searchInput]);

    // Keep secondary filters visible while they have active values (e.g. restored
    // from DocumentFiltersContext), even if the user hadn't opened "+ Filter".
    useEffect(() => {
        setPromotedBrowseFilters((prev) =>
            nextPromotedBrowseFilters(prev, {
                status: statusFilter,
                authorUrns: selectedAuthorUrns,
                platformUrns: selectedPlatformUrns,
                tagUrns: selectedTagUrns,
            }),
        );
    }, [statusFilter, selectedAuthorUrns, selectedPlatformUrns, selectedTagUrns]);

    const promoteBrowseFilter = useCallback((key: SecondaryBrowseFilter) => {
        setPromotedBrowseFilters((prev) => {
            if (prev.has(key)) return prev;
            const next = new Set(prev);
            next.add(key);
            return next;
        });
    }, []);

    const demoteBrowseFilter = useCallback((key: SecondaryBrowseFilter) => {
        setPromotedBrowseFilters((prev) => {
            if (!prev.has(key)) return prev;
            const next = new Set(prev);
            next.delete(key);
            return next;
        });
    }, []);

    const handleStatusUpdate = useCallback(
        (values: string[]) => {
            const next = (values[0] as DocumentStatusFilter) || DEFAULT_STATUS_FILTER;
            setStatusFilter(next);
            setFilterToAutoOpen(null);
            if (next === DEFAULT_STATUS_FILTER) demoteBrowseFilter('status');
        },
        [setStatusFilter, demoteBrowseFilter],
    );

    const handleAuthorUpdate = useCallback(
        (urns: string[]) => {
            setSelectedAuthorUrns(urns);
            setFilterToAutoOpen(null);
            if (urns.length === 0) demoteBrowseFilter('author');
        },
        [setSelectedAuthorUrns, demoteBrowseFilter],
    );

    const handleSourceUpdate = useCallback(
        (urns: string[]) => {
            setSelectedPlatformUrns(urns);
            setFilterToAutoOpen(null);
            if (urns.length === 0) demoteBrowseFilter('source');
        },
        [setSelectedPlatformUrns, demoteBrowseFilter],
    );

    const handleAddBrowseFilter = useCallback(
        (value: string) => {
            if (!isSecondaryBrowseFilter(value)) return;
            // SidebarAddFilter already waits for the menu to close before calling onAdd.
            promoteBrowseFilter(value);
            setFilterToAutoOpen(value);
            setAutoOpenNonce((n) => n + 1);
        },
        [promoteBrowseFilter],
    );

    const handleTagsChange = useCallback(
        (urns: string[]) => {
            setSelectedTagUrns(urns);
            setFilterToAutoOpen(null);
            if (urns.length === 0) demoteBrowseFilter('tag');
        },
        [demoteBrowseFilter],
    );

    const searchModeInput = useMemo(
        () => ({
            typeNames: selectedTypeNames,
            domainUrns: selectedDomainUrns,
            tagUrns: selectedTagUrns,
            termUrns: selectedTermUrns,
            authorUrns: selectedAuthorUrns,
            platformUrns: selectedPlatformUrns,
            status: statusFilter,
        }),
        [
            selectedTypeNames,
            selectedDomainUrns,
            selectedTagUrns,
            selectedTermUrns,
            selectedAuthorUrns,
            selectedPlatformUrns,
            statusFilter,
        ],
    );

    // Chrome switches on immediate input so typing doesn't leave the tree until debounce.
    const isSearchActive = isDocumentSidebarSearchActive({
        ...searchModeInput,
        searchInput,
    });
    // Fetch only after debounce (or when filters alone activate search) to avoid * queries mid-type.
    const shouldFetchSearch = isDocumentSidebarSearchActive({
        ...searchModeInput,
        searchInput: debouncedQuery,
    });

    const {
        typeOptions,
        domainOptions,
        tagOptions,
        termOptions,
        authorOptions,
        platformOptions: platformFacetOptions,
    } = useDocumentSidebarFacetOptions({
        searchQuery: debouncedQuery,
        typeNames: selectedTypeNames,
        domainUrns: selectedDomainUrns,
        tagUrns: selectedTagUrns,
        termUrns: selectedTermUrns,
        authorUrns: selectedAuthorUrns,
        platformUrns: selectedPlatformUrns,
        status: statusFilter,
        viewUrn,
        includeTagFacets: promotedBrowseFilters.has('tag') || selectedTagUrns.length > 0,
        includeAuthorFacets: promotedBrowseFilters.has('author') || selectedAuthorUrns.length > 0,
        includePlatformFacets: promotedBrowseFilters.has('source') || selectedPlatformUrns.length > 0,
    });

    const {
        documents: searchResults,
        total: searchTotal,
        loading: searchLoading,
        isRefreshing: searchRefreshing,
    } = useDocumentSidebarSearch({
        searchQuery: debouncedQuery,
        typeNames: selectedTypeNames,
        domainUrns: selectedDomainUrns,
        tagUrns: selectedTagUrns,
        termUrns: selectedTermUrns,
        authorUrns: selectedAuthorUrns,
        platformUrns: selectedPlatformUrns,
        status: statusFilter,
        sort: sortSelection,
        viewUrn,
        skip: !shouldFetchSearch,
    });

    // Debounce gap (typed but not fetched yet) or in-flight search with no rows yet.
    const searchResultsLoading =
        (isSearchActive && !shouldFetchSearch) || (searchLoading && searchResults.length === 0);

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

    const statusOptions = useMemo(
        () => [
            { value: 'all', label: t('context.statusFilter.all') },
            { value: 'published', label: t('context.statusFilter.published') },
            { value: 'unpublished', label: t('context.statusFilter.unpublished') },
        ],
        [t],
    );

    const sourceOptions = useMemo(
        () =>
            platformFacetOptions.map((option) => ({
                ...option,
                icon: isDataPlatformEntity(option.entity) ? (
                    <DocumentSourceLogo platform={option.entity} size={16} />
                ) : undefined,
            })),
        [platformFacetOptions],
    );

    const sortOptions = useMemo(
        () => [
            { value: DOCUMENT_SIDEBAR_SORT.LAST_MODIFIED_DESC, label: t('sidebarSort.lastModified') },
            { value: DOCUMENT_SIDEBAR_SORT.NAME_ASC, label: t('sidebarSort.nameAtoZ') },
            { value: DOCUMENT_SIDEBAR_SORT.NAME_DESC, label: t('sidebarSort.nameZtoA') },
        ],
        [t],
    );

    const addFilterOptions = useMemo(() => {
        const labels: Record<SecondaryBrowseFilter, string> = {
            tag: t('context.tagFilter.label'),
            status: t('context.statusFilter.label'),
            author: t('context.authorFilter.label'),
            source: t('context.sourceFilter.label'),
        };
        return SECONDARY_BROWSE_FILTERS.filter((key) => !promotedBrowseFilters.has(key)).map((key) => ({
            value: key,
            label: labels[key],
        }));
    }, [promotedBrowseFilters, t]);

    const handleCreateDocument = useCallback(
        async (parentDocumentUrn?: string) => {
            if (!canCreateDocuments) return;

            setCreating(true);
            try {
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

    const handleClearSearch = useCallback(() => {
        setSearchInput('');
        setDebouncedQuery('');
        setSelectedTypeNames([]);
        setSelectedDomainUrns([]);
        setSelectedTagUrns([]);
        setSelectedTermUrns([]);
        setStatusFilter(DEFAULT_STATUS_FILTER);
        setSelectedAuthorUrns([]);
        setSelectedPlatformUrns([]);
        setPromotedBrowseFilters(new Set());
        setFilterToAutoOpen(null);
    }, [setStatusFilter, setSelectedAuthorUrns, setSelectedPlatformUrns]);

    const handleDocumentClick = useCallback(
        (urn: string) => {
            setSuppressSelectionScroll(false);
            // Navigate only — keep query/filters so the user can open another result
            // without re-applying. "Clear search" is the explicit exit back to the tree.
            const url = entityRegistry.getEntityUrl(EntityType.Document, urn);
            history.push(url);
        },
        [entityRegistry, history],
    );

    const handleImportSuccess = useDocumentImportSuccess({ loadChildren });

    const headerActions = (
        <>
            {canCreateDocuments && (
                <ImportDocumentsButton onSuccess={handleImportSuccess} parentDocumentUrn={importParentDocumentUrn} />
            )}
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
                    <SidebarCreateButton
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
        </>
    );

    return (
        <HierarchicalBrowseSidebar
            title={t('context.documentsSidebarTitle')}
            isCollapsed={isCollapsed}
            onToggleCollapsed={onToggleCollapsed}
            onExpandSidebar={onExpandSidebar}
            onWidthChange={onWidthChange}
            headerActions={headerActions}
            dataTestId="context-documents-sidebar"
            collapseButtonTestId="context-sidebar-collapse-button"
            collapsedSearchAriaLabel={t('context.searchDocumentsAriaLabel')}
            collapsedSearchTestId="context-sidebar-search-icon"
            search={
                <SearchBar
                    placeholder={t('context.searchDocumentsPlaceholder')}
                    value={searchInput}
                    onChange={setSearchInput}
                    data-testid="context-sidebar-search-input"
                />
            }
            sort={
                <SidebarSortSelect
                    options={sortOptions}
                    value={sortSelection}
                    onChange={(next) => setSortSelection(next as DocumentSidebarSortValue)}
                    dataTestId="context-sidebar-sort"
                />
            }
            filters={
                <>
                    <DocumentSidebarSearchFilters
                        selectedTypeNames={selectedTypeNames}
                        selectedDomainUrns={selectedDomainUrns}
                        selectedTagUrns={selectedTagUrns}
                        selectedTermUrns={selectedTermUrns}
                        typeOptions={typeOptions}
                        domainOptions={domainOptions}
                        tagOptions={tagOptions}
                        termOptions={termOptions}
                        onTypesChange={setSelectedTypeNames}
                        onDomainsChange={setSelectedDomainUrns}
                        onTagsChange={handleTagsChange}
                        onTermsChange={setSelectedTermUrns}
                        showTagFilter={promotedBrowseFilters.has('tag')}
                        defaultOpenTagFilter={filterToAutoOpen === 'tag'}
                        tagFilterKey={filterToAutoOpen === 'tag' ? `tag-${autoOpenNonce}` : 'tag'}
                    />
                    {/* Tag / Status / Author / Source — via "+ Filter". Any of these
                        (plus Domain/Term/Type) enter corpus-wide search mode. */}
                    {promotedBrowseFilters.has('status') && (
                        <SimpleSelect
                            key={filterToAutoOpen === 'status' ? `status-${autoOpenNonce}` : 'status'}
                            size="sm"
                            width="fit-content"
                            showClear={false}
                            defaultOpen={filterToAutoOpen === 'status'}
                            placeholder={t('context.statusFilter.placeholder')}
                            selectLabelProps={{ variant: 'default' }}
                            options={statusOptions}
                            values={[statusFilter]}
                            onUpdate={handleStatusUpdate}
                            dataTestId="context-sidebar-status-filter"
                        />
                    )}
                    {promotedBrowseFilters.has('author') && (
                        <SimpleSelect
                            key={filterToAutoOpen === 'author' ? `author-${autoOpenNonce}` : 'author'}
                            size="sm"
                            width="fit-content"
                            isMultiSelect
                            defaultOpen={filterToAutoOpen === 'author'}
                            isDisabled={
                                authorOptions.length === 0 &&
                                selectedAuthorUrns.length === 0 &&
                                filterToAutoOpen !== 'author'
                            }
                            placeholder={t('context.authorFilter.placeholder')}
                            selectLabelProps={{ variant: 'labeled', label: t('context.authorFilter.label') }}
                            options={authorOptions}
                            values={selectedAuthorUrns}
                            onUpdate={handleAuthorUpdate}
                            renderCustomOptionText={(option) => {
                                const { creator } = option as (typeof authorOptions)[number];
                                return (
                                    <Avatar
                                        name={creator.displayName}
                                        imageUrl={creator.pictureLink ?? undefined}
                                        type={
                                            creator.type === EntityType.CorpGroup ? AvatarType.group : AvatarType.user
                                        }
                                        showInPill
                                        size="sm"
                                    />
                                );
                            }}
                            dataTestId="context-sidebar-author-filter"
                        />
                    )}
                    {promotedBrowseFilters.has('source') && (
                        <SimpleSelect
                            key={filterToAutoOpen === 'source' ? `source-${autoOpenNonce}` : 'source'}
                            size="sm"
                            width="fit-content"
                            isMultiSelect
                            defaultOpen={filterToAutoOpen === 'source'}
                            isDisabled={
                                sourceOptions.length === 0 &&
                                selectedPlatformUrns.length === 0 &&
                                filterToAutoOpen !== 'source'
                            }
                            placeholder={t('context.sourceFilter.placeholder')}
                            selectLabelProps={{ variant: 'labeled', label: t('context.sourceFilter.label') }}
                            options={sourceOptions}
                            values={selectedPlatformUrns}
                            onUpdate={handleSourceUpdate}
                            renderCustomOptionText={(option) => (
                                <SourceOptionRow>
                                    {option.icon}
                                    <span>{option.label}</span>
                                </SourceOptionRow>
                            )}
                            dataTestId="context-sidebar-source-filter"
                        />
                    )}
                    <SidebarAddFilter
                        options={addFilterOptions}
                        onAdd={handleAddBrowseFilter}
                        dataTestId="context-sidebar-add-filter"
                    />
                </>
            }
        >
            {isSearchActive ? (
                <DocumentSidebarSearchResults
                    documents={searchResults}
                    total={searchTotal}
                    loading={searchResultsLoading}
                    isRefreshing={searchRefreshing}
                    selectedUrn={importParentDocumentUrn}
                    onSelect={handleDocumentClick}
                    onClear={handleClearSearch}
                    onCreateChild={canCreateDocuments ? (parentUrn) => handleCreateDocument(parentUrn) : undefined}
                />
            ) : (
                <DocumentTree
                    key={sortSelection}
                    onCreateChild={(parentUrn) => handleCreateDocument(parentUrn || undefined)}
                    sortSelection={sortSelection}
                    suppressSelectionScroll={suppressSelectionScroll}
                />
            )}
        </HierarchicalBrowseSidebar>
    );
}
