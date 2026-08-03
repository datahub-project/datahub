import { Loader } from '@components';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import NodeItem from '@app/glossaryV2/GlossaryBrowser/NodeItem';
import TermItem from '@app/glossaryV2/GlossaryBrowser/TermItem';
import { ROOT_NODES, ROOT_TERMS } from '@app/glossaryV2/utils';
import {
    TreeExpansionRegistryProvider,
    useTreeExpansionRegistry,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeExpansionRegistry';
import { TreeSectionHeader } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeSectionHeader';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GlossaryNodeFragment } from '@graphql/fragments.generated';
import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '@graphql/glossary.generated';
import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';

// Picker embeds keep a local scroll wrapper. Sidebar use relies on the shared
// HierarchicalBrowseSidebar TreeContainer instead.
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

    const [isAllTermsExpanded, setIsAllTermsExpanded] = useState(true);
    // Expand-all while the section is collapsed is a no-op (nodes unmounted);
    // defer until after they register on the next paint.
    const pendingExpandAllRef = useRef(false);

    const isSidebarUse = !isSelecting;
    const showTreeContents = !isSidebarUse || isAllTermsExpanded;

    const {
        data: nodesData,
        refetch: refetchNodes,
        loading: nodesLoading,
    } = useGetRootGlossaryNodesQuery({ skip: !!rootNodes });
    const {
        data: termsData,
        refetch: refetchTerms,
        loading: termsLoading,
    } = useGetRootGlossaryTermsQuery({ skip: !!rootTerms });
    const loading = nodesLoading || termsLoading;

    const fetchedNodes = useMemo(
        () => rootNodes || nodesData?.getRootGlossaryNodes?.nodes || [],
        [rootNodes, nodesData],
    );
    const fetchedTerms = useMemo(
        () => rootTerms || termsData?.getRootGlossaryTerms?.terms || [],
        [rootTerms, termsData],
    );

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

    const entityRegistry = useEntityRegistry();
    const sortedNodes = useMemo(
        () => displayedNodes.slice().sort((a, b) => sortGlossaryNodes(entityRegistry, a, b)),
        [displayedNodes, entityRegistry],
    );
    const sortedTerms = useMemo(
        () => displayedTerms.slice().sort((a, b) => sortGlossaryTerms(entityRegistry, a, b)),
        [displayedTerms, entityRegistry],
    );

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
        if (refreshBrowser) {
            refetchNodes();
            refetchTerms();
        }
    }, [refreshBrowser, refetchNodes, refetchTerms]);

    useEffect(() => {
        if (urnsToUpdate.includes(ROOT_NODES)) {
            refetchNodes();
            setUrnsToUpdate((prev) => prev.filter((urn) => urn !== ROOT_NODES));
        }
        if (urnsToUpdate.includes(ROOT_TERMS)) {
            refetchTerms();
            setUrnsToUpdate((prev) => prev.filter((urn) => urn !== ROOT_TERMS));
        }
    }, [urnsToUpdate, setUrnsToUpdate, refetchNodes, refetchTerms]);

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
    }, [isAllTermsExpanded, expansion, sortedNodes, sortedTerms]);

    const tree = (
        <>
            {isSidebarUse && (
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
            {showTreeContents && (
                <>
                    {sortedNodes.map((node) => (
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
                        sortedTerms.map((term) => (
                            <TermItem
                                key={term.urn}
                                term={term}
                                isSelecting={isSelecting}
                                selectTerm={selectTerm}
                                depth={0}
                            />
                        ))}
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
