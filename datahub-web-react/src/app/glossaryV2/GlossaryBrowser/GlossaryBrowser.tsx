import { Loader } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
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

// 8px gutter on all sides — matches `NavigatorWrapper` in the domains sidebar
// and `TreeContainer` in the documents sidebar so the row chrome (selected
// highlight, hover shadow) sits inset from the sidebar edge.
//
// Custom 6px scrollbar (vs. the OS default ~15px on macOS) — without this,
// the scrollbar visibly eats into the 8px right padding and makes the
// right edge look uneven. Uses the semantic `scrollbarThumb` /
// `scrollbarThumbHover` / `scrollbarTrack` tokens (gray100 → gray500 on
// hover in light mode) so it matches `MoveDocumentPopover` and stays
// subtle by default.
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

// --- Section header ---------------------------------------------------------
// "All Glossary Terms" group label at the top of the tree. Styling matches
// the documents sidebar's per-platform `SectionHeader` and the domains
// sidebar's "All Domains" header — Mulish-700 / textTertiary, right-side
// caret, 32px min row height, level-based indent (8 + level*16). Acts as a
// pure collapsible group header (no navigation), again matching the other
// two sidebars.
// No hover background on the section header — it's a tree label, not a
// nav row; the pointer cursor alone is enough affordance for the toggle.
// (The documents sidebar's equivalent does add a hover bg; we're choosing
// the cleaner treatment for glossary.)
const SectionHeader = styled.button<{ $level: number }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    width: 100%;
    padding: 6px 8px 6px ${(props) => 8 + props.$level * 16}px;
    min-height: 32px;
    border: none;
    background: transparent;
    cursor: pointer;
    text-align: left;
    color: ${(props) => props.theme.colors.textTertiary};
    font-family: Mulish;
    font-size: 14px;
    font-weight: 700;
`;

const SectionHeaderLabel = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

/**
 * Collapsible section header — pure presentation, no expansion state of its
 * own. Mirrors `DomainSectionHeader` in the domains navigator and
 * `TreeSectionHeader` in the documents sidebar so a level-0 "All Glossary
 * Terms" header lines up with a level-0 "All Domains" / "DataHub" header
 * pixel-for-pixel.
 */
function GlossarySectionHeader({
    level,
    label,
    isExpanded,
    onToggle,
    testId,
}: {
    level: number;
    label: string;
    isExpanded: boolean;
    onToggle: () => void;
    testId?: string;
}) {
    const Chevron = isExpanded ? CaretDown : CaretRight;
    return (
        <SectionHeader type="button" $level={level} onClick={onToggle} aria-expanded={isExpanded} data-testid={testId}>
            <SectionHeaderLabel>{label}</SectionHeaderLabel>
            <Chevron size={14} weight="regular" />
        </SectionHeader>
    );
}

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

    const { t } = useTranslation('governance.glossary');
    const { urnsToUpdate, setUrnsToUpdate, nodeToNewEntity, setNodeToNewEntity } = useGlossaryEntityData();

    // Section expansion state — local to the component. Defaults open; toggling
    // the "All Glossary Terms" header hides the tree (matches `DomainNavigator`
    // and the docs sidebar's per-platform section headers).
    const [isAllTermsExpanded, setIsAllTermsExpanded] = useState(true);

    // Picker variants (AddRelatedTermsModal, GlossarySelector) embed this
    // browser inside their own dropdown and don't get a section header. The
    // sidebar variant gets the collapsible header; everything else renders
    // the tree directly.
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
        if (fetchedNodes.some((node) => node.urn === optimisticRootNode.urn)) return fetchedNodes;
        return [optimisticRootNode, ...fetchedNodes];
    }, [fetchedNodes, optimisticRootNode]);

    const displayedTerms = useMemo(() => {
        if (!optimisticRootTerm) return fetchedTerms;
        if (fetchedTerms.some((term) => term.urn === optimisticRootTerm.urn)) return fetchedTerms;
        return [optimisticRootTerm, ...fetchedTerms];
    }, [fetchedTerms, optimisticRootTerm]);

    const entityRegistry = useEntityRegistry();
    // Memoize so we don't allocate fresh sorted arrays on every render — `NodeItem` and
    // `TermItem` props would otherwise change identity every tick and defeat any downstream
    // memoization.
    const sortedNodes = useMemo(
        () => displayedNodes.slice().sort((a, b) => sortGlossaryNodes(entityRegistry, a, b)),
        [displayedNodes, entityRegistry],
    );
    const sortedTerms = useMemo(
        () => displayedTerms.slice().sort((a, b) => sortGlossaryTerms(entityRegistry, a, b)),
        [displayedTerms, entityRegistry],
    );

    // Drop the optimistic root entry once the canonical fetched data contains the URN, so
    // displayedNodes flips from `[optimistic, ...fetchedNodes]` to plain `fetchedNodes` (which
    // now carries the real server-side fields like childrenCount, parentNodes, etc.).
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

    // If node(s) or term(s) need to be refreshed at the root level, check if these special
    // cases are in `urnsToUpdate`. Functional setter (not `urnsToUpdate.filter(...)`) so we
    // don't strip the wrong subset when multiple updates are queued in the same render —
    // `urnsToUpdate` from the closure can be stale by the time React applies our update.
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

    return (
        <BrowserWrapper>
            {isSidebarUse && (
                <GlossarySectionHeader
                    level={0}
                    label={t('sidebar.section.allTerms')}
                    isExpanded={isAllTermsExpanded}
                    onToggle={() => setIsAllTermsExpanded((v) => !v)}
                    testId="glossary-sidebar-section-all-terms"
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
        </BrowserWrapper>
    );
}

export default GlossaryBrowser;
