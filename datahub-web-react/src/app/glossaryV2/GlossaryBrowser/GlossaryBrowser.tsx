import { LoadingOutlined } from '@ant-design/icons';
import React, { useEffect, useMemo } from 'react';
import styled from 'styled-components/macro';

import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import NodeItem from '@app/glossaryV2/GlossaryBrowser/NodeItem';
import TermItem from '@app/glossaryV2/GlossaryBrowser/TermItem';
import { ROOT_NODES, ROOT_TERMS } from '@app/glossaryV2/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { GlossaryNodeFragment } from '@graphql/fragments.generated';
import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '@graphql/glossary.generated';
import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';

const BrowserWrapper = styled.div`
    font-size: 12px;
    max-height: calc(100% - 104px);
    padding: 0;
    overflow-y: auto;
    overflow-x: hidden;
`;

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;

    svg {
        height: 15px;
        width: 15px;
        color: ${(props) => props.theme.colors.icon};
    }
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

function GlossaryBrowser(props: Props) {
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

    const { urnsToUpdate, setUrnsToUpdate, nodeToNewEntity, setNodeToNewEntity } = useGlossaryEntityData();

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

    // Stabilize via useMemo so dependent useMemos/useEffects don't re-run on every render —
    // the `||` fallback to `[]` would otherwise produce a fresh array reference each call.
    const fetchedNodes = useMemo(
        () => rootNodes || nodesData?.getRootGlossaryNodes?.nodes || [],
        [rootNodes, nodesData],
    );
    const fetchedTerms = useMemo(
        () => rootTerms || termsData?.getRootGlossaryTerms?.terms || [],
        [rootTerms, termsData],
    );

    // Optimistic root entries: when CreateGlossaryEntityModal creates a top-level term/term-group,
    // it stashes the new entity in `nodeToNewEntity[ROOT_NODES]` / `nodeToNewEntity[ROOT_TERMS]`.
    // The backend's `getRootGlossaryNodes` / `getRootGlossaryTerms` resolvers rely on a search
    // index that lags behind the mutation by several seconds, so the post-create refetch often
    // returns the list WITHOUT the new entity. Without this optimistic prepend the user sees
    // "created" but no sidebar entry until the index catches up (sometimes >5s, sometimes never
    // until they refresh). Cleared as soon as the refetched data contains the URN — see effects
    // below — so we don't accumulate stale entries if a creation actually failed downstream.
    const optimisticRootNode = nodeToNewEntity[ROOT_NODES] as GlossaryNodeFragment | undefined;
    const optimisticRootTerm = nodeToNewEntity[ROOT_TERMS] as ChildGlossaryTermFragment | undefined;

    const displayedNodes = useMemo(() => {
        if (!optimisticRootNode) return fetchedNodes;
        if (fetchedNodes.some((n) => n.urn === optimisticRootNode.urn)) return fetchedNodes;
        return [optimisticRootNode, ...fetchedNodes];
    }, [fetchedNodes, optimisticRootNode]);

    const displayedTerms = useMemo(() => {
        if (!optimisticRootTerm) return fetchedTerms;
        if (fetchedTerms.some((t) => t.urn === optimisticRootTerm.urn)) return fetchedTerms;
        return [optimisticRootTerm, ...fetchedTerms];
    }, [fetchedTerms, optimisticRootTerm]);

    const entityRegistry = useEntityRegistry();
    const sortedNodes = displayedNodes.slice().sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA, nodeB));
    const sortedTerms = displayedTerms.slice().sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA, termB));

    // Drop the optimistic root entry once the canonical fetched data contains the URN, so
    // displayedNodes flips from `[optimistic, ...fetchedNodes]` to plain `fetchedNodes` (which
    // now carries the real server-side fields like childrenCount, parentNodes, etc.).
    useEffect(() => {
        if (optimisticRootNode && fetchedNodes.some((n) => n.urn === optimisticRootNode.urn)) {
            setNodeToNewEntity((prev) => {
                const next = { ...prev };
                delete next[ROOT_NODES];
                return next;
            });
        }
    }, [optimisticRootNode, fetchedNodes, setNodeToNewEntity]);

    useEffect(() => {
        if (optimisticRootTerm && fetchedTerms.some((t) => t.urn === optimisticRootTerm.urn)) {
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

    // if node(s) or term(s) need to be refreshed at the root level, check if these special cases are in `urnsToUpdate`
    useEffect(() => {
        if (urnsToUpdate.includes(ROOT_NODES)) {
            refetchNodes();
            setUrnsToUpdate(urnsToUpdate.filter((urn) => urn !== ROOT_NODES));
        }
        if (urnsToUpdate.includes(ROOT_TERMS)) {
            refetchTerms();
            setUrnsToUpdate(urnsToUpdate.filter((urn) => urn !== ROOT_TERMS));
        }
    });

    return (
        <BrowserWrapper>
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
                    <TermItem key={term.urn} term={term} isSelecting={isSelecting} selectTerm={selectTerm} depth={0} />
                ))}
            {loading && (
                <LoadingWrapper>
                    <LoadingOutlined />
                </LoadingWrapper>
            )}
        </BrowserWrapper>
    );
}

export default GlossaryBrowser;
