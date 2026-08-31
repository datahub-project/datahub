import { Loader } from '@components';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import GlossaryFlatItem from '@app/glossaryV2/GlossaryBrowser/GlossaryFlatItem';
import NodeItem from '@app/glossaryV2/GlossaryBrowser/NodeItem';
import TermItem from '@app/glossaryV2/GlossaryBrowser/TermItem';
import { useGlossarySidebarFilters } from '@app/glossaryV2/glossarySidebarFilters/GlossarySidebarFiltersContext';
import useGlossaryDomainAggregations from '@app/glossaryV2/glossarySidebarFilters/useGlossaryDomainAggregations';
import useGlossaryOwnerAggregations from '@app/glossaryV2/glossarySidebarFilters/useGlossaryOwnerAggregations';
import useGlossaryTagAggregations from '@app/glossaryV2/glossarySidebarFilters/useGlossaryTagAggregations';
import useScrollGlossaryEntities from '@app/glossaryV2/glossarySidebarFilters/useScrollGlossaryEntities';
import { ROOT_NODES, ROOT_TERMS } from '@app/glossaryV2/utils';
import SidebarFilteredResults from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarFilteredResults';
import {
    TreeExpansionRegistryProvider,
    useTreeExpansionRegistry,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeExpansionRegistry';
import { TreeSectionHeader } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeSectionHeader';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GlossaryNodeFragment } from '@graphql/fragments.generated';
import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '@graphql/glossary.generated';
import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';
import { EntityType } from '@types';

const BrowserWrapper = styled.div`
    max-height: calc(100% - 104px);
    padding: 8px;
    overflow-y: auto;
    overflow-x: hidden;

    &::-webkit-scrollbar {
        width: 6px;
    }

    &::-webkit-scrollbar-track {
        background: ${(props) => props.theme.colors.scrollbarTrack};
    }

    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb:hover {
        background: ${(props) => props.theme.colors.scrollbarThumbHover};
    }

    scrollbar-width: thin;
    scrollbar-color: ${(props) => `${props.theme.colors.scrollbarThumb} ${props.theme.colors.scrollbarTrack}`};
`;

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;
`;

interface Props {
    rootNodes?: GlossaryNodeFragment[];
    rootTerms?: ChildGlossaryTermFragment[];
    isSelecting?: boolean;
    hideTerms?: boolean;
    openToEntity?: boolean;
    refreshBrowser?: boolean;
    nodeUrnToHide?: string;
    selectTerm?: (urn: string, displayName: string) => void;
    selectNode?: (urn: string, displayName: string) => void;
    selectedUrns?: string[];
}

function GlossaryBrowserInner(props: Props) {
    const {
        rootNodes,
        rootTerms,
        isSelecting,
        hideTerms,
        refreshBrowser,
        openToEntity,
        nodeUrnToHide,
        selectTerm,
        selectNode,
        selectedUrns,
    } = props;

    const { t } = useTranslation('governance.glossary');
    const { t: tm } = useTranslation('misc');
    const { urnsToUpdate, setUrnsToUpdate, nodeToNewEntity, setNodeToNewEntity } = useGlossaryEntityData();
    const expansion = useTreeExpansionRegistry();
    const {
        selectedOwnerUrns,
        selectedTagUrns,
        selectedDomainUrns,
        setSelectedOwnerUrns,
        setSelectedTagUrns,
        setSelectedDomainUrns,
        setAvailableOwners,
        setAvailableTags,
        setAvailableDomains,
        sortSelection,
    } = useGlossarySidebarFilters();
    const entityRegistry = useEntityRegistry();

    const [isAllTermsExpanded, setIsAllTermsExpanded] = useState(true);
    const pendingExpandAllRef = useRef(false);

    const isSidebarUse = !isSelecting;
    const isFiltering =
        isSidebarUse && (selectedOwnerUrns.length > 0 || selectedTagUrns.length > 0 || selectedDomainUrns.length > 0);
    /** Sidebar tree roots come from scrollAcrossEntities + sortInput — never client-sorted. */
    const useServerSortedRoots = isSidebarUse && !isFiltering;
    const showTreeContents = !isSidebarUse || isAllTermsExpanded || isFiltering;

    const {
        data: nodesData,
        refetch: refetchNodes,
        loading: nodesLoading,
    } = useGetRootGlossaryNodesQuery({
        // Pickers (and optional props) keep the legacy root queries; sidebar uses scroll.
        skip: isSidebarUse || !!rootNodes,
    });
    const {
        data: termsData,
        refetch: refetchTerms,
        loading: termsLoading,
    } = useGetRootGlossaryTermsQuery({
        skip: isSidebarUse || !!rootTerms,
    });

    const {
        entities: serverRootEntities,
        loading: serverRootsLoading,
        scrollRef: serverRootsScrollRef,
        refetch: refetchServerRoots,
    } = useScrollGlossaryEntities({
        skip: !useServerSortedRoots,
        parentNode: null,
        sort: sortSelection,
        sortTypeBeforeName: true,
    });

    const {
        entities: filteredEntities,
        loading: filteredLoading,
        scrollRef: filteredScrollRef,
    } = useScrollGlossaryEntities({
        skip: !isFiltering,
        sort: sortSelection,
        selectedOwnerUrns,
        selectedTagUrns,
        selectedDomainUrns,
        ignoreParentScope: true,
        sortTypeBeforeName: false,
    });

    const { owners: aggregatedOwners } = useGlossaryOwnerAggregations({ skip: !isSidebarUse });
    const { tags: aggregatedTags } = useGlossaryTagAggregations({ skip: !isSidebarUse });
    const { domains: aggregatedDomains } = useGlossaryDomainAggregations({ skip: !isSidebarUse });

    useEffect(() => {
        if (!isSidebarUse) return;
        setAvailableOwners(aggregatedOwners);
    }, [isSidebarUse, aggregatedOwners, setAvailableOwners]);

    useEffect(() => {
        if (!isSidebarUse) return;
        setAvailableTags(aggregatedTags);
    }, [isSidebarUse, aggregatedTags, setAvailableTags]);

    useEffect(() => {
        if (!isSidebarUse) return;
        setAvailableDomains(aggregatedDomains);
    }, [isSidebarUse, aggregatedDomains, setAvailableDomains]);

    let loading = nodesLoading || termsLoading;
    if (isFiltering) {
        loading = filteredLoading;
    } else if (useServerSortedRoots) {
        loading = serverRootsLoading;
    }

    // Preserve server order — filter by type only, do not re-sort.
    const fetchedNodes = useMemo((): GlossaryNodeFragment[] => {
        if (useServerSortedRoots) {
            return serverRootEntities.filter((e) => e.type === EntityType.GlossaryNode) as GlossaryNodeFragment[];
        }
        return rootNodes || nodesData?.getRootGlossaryNodes?.nodes || [];
    }, [useServerSortedRoots, serverRootEntities, rootNodes, nodesData]);

    const fetchedTerms = useMemo((): ChildGlossaryTermFragment[] => {
        if (useServerSortedRoots) {
            return serverRootEntities.filter((e) => e.type === EntityType.GlossaryTerm) as ChildGlossaryTermFragment[];
        }
        return rootTerms || termsData?.getRootGlossaryTerms?.terms || [];
    }, [useServerSortedRoots, serverRootEntities, rootTerms, termsData]);

    const optimisticRootNode = nodeToNewEntity[ROOT_NODES] as GlossaryNodeFragment | undefined;
    const optimisticRootTerm = nodeToNewEntity[ROOT_TERMS] as ChildGlossaryTermFragment | undefined;

    const displayedNodes = useMemo(() => {
        if (!optimisticRootNode) return fetchedNodes;
        if (fetchedNodes.some((node) => node.urn === optimisticRootNode.urn)) return fetchedNodes;
        return [optimisticRootNode, ...fetchedNodes];
    }, [fetchedNodes, optimisticRootNode]);

    const displayedTerms = useMemo(() => {
        if (!optimisticRootTerm) return fetchedTerms;
        if (fetchedTerms.some((term) => term.urn === optimisticRootTerm.urn)) return fetchedTerms;
        return [optimisticRootTerm, ...fetchedTerms];
    }, [fetchedTerms, optimisticRootTerm]);

    // Pickers only: legacy client A–Z. Sidebar trusts scrollAcrossEntities sortInput.
    const treeNodes = useMemo(() => {
        if (useServerSortedRoots) return displayedNodes;
        return displayedNodes.slice().sort((a, b) => sortGlossaryNodes(entityRegistry, a, b));
    }, [useServerSortedRoots, displayedNodes, entityRegistry]);

    const treeTerms = useMemo(() => {
        if (useServerSortedRoots) return displayedTerms;
        return displayedTerms.slice().sort((a, b) => sortGlossaryTerms(entityRegistry, a, b));
    }, [useServerSortedRoots, displayedTerms, entityRegistry]);

    useEffect(() => {
        if (optimisticRootNode && fetchedNodes.some((node) => node.urn === optimisticRootNode.urn)) {
            setNodeToNewEntity((prev) => {
                const next = { ...prev };
                delete next[ROOT_NODES];
                return next;
            });
        }
    }, [optimisticRootNode, fetchedNodes, setNodeToNewEntity]);

    useEffect(() => {
        if (optimisticRootTerm && fetchedTerms.some((term) => term.urn === optimisticRootTerm.urn)) {
            setNodeToNewEntity((prev) => {
                const next = { ...prev };
                delete next[ROOT_TERMS];
                return next;
            });
        }
    }, [optimisticRootTerm, fetchedTerms, setNodeToNewEntity]);

    useEffect(() => {
        if (!refreshBrowser || isFiltering) return;
        if (useServerSortedRoots) {
            refetchServerRoots();
            return;
        }
        refetchNodes();
        refetchTerms();
    }, [refreshBrowser, refetchNodes, refetchTerms, refetchServerRoots, isFiltering, useServerSortedRoots]);

    useEffect(() => {
        if (isFiltering) return;
        const needsRootRefresh = urnsToUpdate.includes(ROOT_NODES) || urnsToUpdate.includes(ROOT_TERMS);
        if (!needsRootRefresh) return;

        if (useServerSortedRoots) {
            refetchServerRoots();
        } else {
            if (urnsToUpdate.includes(ROOT_NODES)) refetchNodes();
            if (urnsToUpdate.includes(ROOT_TERMS)) refetchTerms();
        }
        setUrnsToUpdate((prev) => prev.filter((urn) => urn !== ROOT_NODES && urn !== ROOT_TERMS));
    }, [
        urnsToUpdate,
        setUrnsToUpdate,
        refetchNodes,
        refetchTerms,
        refetchServerRoots,
        isFiltering,
        useServerSortedRoots,
    ]);

    const handleToggleExpandAll = () => {
        if (!expansion) return;
        if (expansion.hasAnyExpanded) {
            pendingExpandAllRef.current = false;
            expansion.collapseAll();
            return;
        }
        if (!isAllTermsExpanded) {
            pendingExpandAllRef.current = true;
            setIsAllTermsExpanded(true);
            return;
        }
        expansion.expandAll();
    };

    useEffect(() => {
        if (!pendingExpandAllRef.current || !isAllTermsExpanded || !expansion) return;
        pendingExpandAllRef.current = false;
        expansion.expandAll();
    }, [isAllTermsExpanded, expansion, treeNodes, treeTerms]);

    const handleClearFilters = useCallback(() => {
        setSelectedOwnerUrns([]);
        setSelectedTagUrns([]);
        setSelectedDomainUrns([]);
    }, [setSelectedOwnerUrns, setSelectedTagUrns, setSelectedDomainUrns]);

    const showSectionHeader = isSidebarUse && !isFiltering;

    const tree = (
        <>
            {showSectionHeader && (
                <TreeSectionHeader
                    level={0}
                    label={t('sidebar.section.allTerms')}
                    isExpanded={isAllTermsExpanded}
                    onToggle={() => setIsAllTermsExpanded((v) => !v)}
                    testId="glossary-sidebar-section-all-terms"
                    onToggleExpandAll={handleToggleExpandAll}
                    isAllExpanded={expansion?.hasAnyExpanded}
                    expandAllLabel={tm('context.tree.expandAll')}
                    collapseAllLabel={tm('context.tree.collapseAll')}
                />
            )}
            {showTreeContents && isFiltering && (
                <SidebarFilteredResults
                    count={filteredEntities.length}
                    loading={filteredLoading && filteredEntities.length === 0}
                    isRefreshing={filteredLoading && filteredEntities.length > 0}
                    onClear={handleClearFilters}
                    clearTestId="glossary-sidebar-clear-filters"
                    dataTestId="glossary-sidebar-filtered-results"
                >
                    {filteredEntities.map((entity) => (
                        <GlossaryFlatItem key={entity.urn} entity={entity} />
                    ))}
                    <div ref={filteredScrollRef} />
                    {loading && filteredEntities.length > 0 && (
                        <LoadingWrapper>
                            <Loader size="sm" padding={0} />
                        </LoadingWrapper>
                    )}
                </SidebarFilteredResults>
            )}
            {showTreeContents && !isFiltering && (
                <>
                    {treeNodes.map((node) => (
                        <NodeItem
                            key={node.urn}
                            node={node}
                            isSelecting={isSelecting}
                            hideTerms={hideTerms}
                            openToEntity={openToEntity}
                            refreshBrowser={refreshBrowser}
                            nodeUrnToHide={nodeUrnToHide}
                            selectTerm={selectTerm}
                            selectNode={selectNode}
                            depth={0}
                            selectedUrns={selectedUrns}
                        />
                    ))}
                    {!hideTerms &&
                        treeTerms.map((term) => (
                            <TermItem
                                key={term.urn}
                                term={term}
                                isSelecting={isSelecting}
                                selectTerm={selectTerm}
                                depth={0}
                            />
                        ))}
                    {useServerSortedRoots && <div ref={serverRootsScrollRef} />}
                    {loading && (
                        <LoadingWrapper>
                            <Loader size="sm" padding={0} />
                        </LoadingWrapper>
                    )}
                </>
            )}
        </>
    );

    if (isSidebarUse) {
        return tree;
    }

    return <BrowserWrapper>{tree}</BrowserWrapper>;
}

function GlossaryBrowser(props: Props) {
    if (props.isSelecting) {
        return <GlossaryBrowserInner {...props} />;
    }
    return (
        <TreeExpansionRegistryProvider>
            <GlossaryBrowserInner {...props} />
        </TreeExpansionRegistryProvider>
    );
}

export default GlossaryBrowser;
