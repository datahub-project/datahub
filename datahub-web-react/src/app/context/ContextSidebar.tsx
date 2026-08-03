import { Avatar, Loader, SearchBar, Tooltip } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { matchPath, useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';

import { AvatarType } from '@components/components/AvatarStack/types';
import { SimpleSelect } from '@components/components/Select/SimpleSelect';

import ImportDocumentsButton from '@app/context/import/ImportDocumentsButton';
import { useDocumentImportSuccess } from '@app/context/import/hooks/useDocumentImportSuccess';
import { useContextDocumentsPermissions } from '@app/context/useContextDocumentsPermissions';
import { useDocumentFilters } from '@app/document/DocumentFiltersContext';
import { DocumentSourceLogo } from '@app/document/DocumentSourceLogo';
import { useDocumentTree } from '@app/document/DocumentTreeContext';
import { useCreateDocumentTreeMutation } from '@app/document/hooks/useDocumentTreeMutations';
import { useLoadDocumentTree } from '@app/document/hooks/useLoadDocumentTree';
import { useSearchDocuments } from '@app/document/hooks/useSearchDocuments';
import {
    DEFAULT_STATUS_FILTER,
    DocumentStatusFilter,
    getAvailablePlatforms,
    getDistinctCreators,
} from '@app/document/utils/documentTreeFilters';
import { decodeUrn } from '@app/entityV2/shared/utils';
import { DocumentTree } from '@app/homeV2/layout/sidebar/documents/DocumentTree';
import { SearchResultItem } from '@app/homeV2/layout/sidebar/documents/SearchResultItem';
import ClickOutside from '@app/shared/ClickOutside';
import HierarchicalBrowseSidebar from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar';
import {
    SearchResultsDropdown,
    SidebarCreateButton,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DocumentSourceType, DocumentState, EntityType } from '@types';

// URN prefix used to identify AI-agent actors. Documents authored by an agent
// are filtered out of the Author multi-select in OSS — agents aren't a first-
// class concept here, so they would render as orphan "human" rows.
const AI_AGENT_URN_PREFIX = 'urn:li:aiAgent:';

const SearchWrapper = styled.div`
    position: relative;
`;

const LoadingWrapper = styled(SearchResultsDropdown)`
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
    const { t: tc } = useTranslation('common.actions');
    const { t: tet } = useTranslation('entity.types');
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
    const { loadChildren } = useLoadDocumentTree();
    const history = useHistory();
    const location = useLocation();
    const entityRegistry = useEntityRegistry();

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
                <SearchWrapper>
                    <ClickOutside onClickOutside={() => setIsSearchBarFocused(false)}>
                        <SearchBar
                            placeholder={t('context.searchDocumentsPlaceholder')}
                            value={searchInput}
                            onChange={setSearchInput}
                            onFocus={() => setIsSearchBarFocused(true)}
                            data-testid="context-sidebar-search-input"
                        />
                        {searchLoading && isSearchBarFocused && isSearching && (
                            <LoadingWrapper>
                                <Loader size="md" />
                            </LoadingWrapper>
                        )}
                        {!searchLoading && isSearchBarFocused && isSearching && searchResults.length === 0 && (
                            <SearchResultsDropdown>
                                <EmptyState>{tc('noResults')}</EmptyState>
                            </SearchResultsDropdown>
                        )}
                        {!searchLoading && isSearchBarFocused && isSearching && searchResults.length > 0 && (
                            <SearchResultsDropdown data-testid="context-sidebar-search-results">
                                {searchResults.map((doc) => {
                                    // Build breadcrumb from parentDocuments array
                                    let breadcrumb: string | null = null;
                                    /* eslint-disable i18next/no-literal-string -- (untranslated-text) ' > ' is a decorative punctuation separator between breadcrumb segments */
                                    if (doc.parentDocuments?.documents && doc.parentDocuments.documents.length > 0) {
                                        const parents = [...doc.parentDocuments.documents].reverse();
                                        breadcrumb = parents
                                            .map((parent) => parent.info?.title || tet('document.untitledFallback'))
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
                            </SearchResultsDropdown>
                        )}
                    </ClickOutside>
                </SearchWrapper>
            }
            filters={
                <>
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
                </>
            }
        >
            <DocumentTree
                onCreateChild={(parentUrn) => handleCreateDocument(parentUrn || undefined)}
                filterSelection={filterSelection}
            />
        </HierarchicalBrowseSidebar>
    );
}
