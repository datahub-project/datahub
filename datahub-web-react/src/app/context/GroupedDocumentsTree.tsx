import { EmptyState } from '@components';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import DocumentGroupSection from '@app/context/DocumentGroupSection';
import useDocumentGroupRoots from '@app/document/hooks/useDocumentGroupRoots';
import { useDocumentNavigation } from '@app/document/hooks/useDocumentNavigation';
import {
    resolveActiveDocumentDomainGroup,
    shouldRefetchDocumentDomainGroups,
    toDocumentGroupingEntityData,
} from '@app/document/utils/documentSidebarGrouping';
import { DocumentSidebarSortValue } from '@app/document/utils/documentSidebarSort';
import Loading from '@app/sharedV2/Loading';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetDocumentQuery } from '@graphql/document.generated';
import { Document } from '@types';

const EmptyStateWrapper = styled.div`
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px 12px;
`;

type Props = {
    sort: DocumentSidebarSortValue;
    viewUrn?: string | null;
    /** After sort/grouping changes, keep the list pinned at the top. */
    suppressSelectionScroll?: boolean;
    onCreateChild?: (parentUrn: string) => void;
    onSelect: (urn: string) => void;
    /**
     * Open document moved between domain groups. Parent remounts this tree
     * (`domainGroupsRefreshKey`) so expanded group searches refetch. The
     * selected-document overlay only hides the move while that doc stays open.
     */
    onDocumentMovedBetweenGroups?: () => void;
};

export default function GroupedDocumentsTree({
    sort,
    viewUrn,
    suppressSelectionScroll = false,
    onCreateChild,
    onSelect,
    onDocumentMovedBetweenGroups,
}: Props) {
    const { t } = useTranslation('misc');
    const entityRegistry = useEntityRegistry();
    const { getCurrentDocumentUrn } = useDocumentNavigation();
    const selectedUrn = getCurrentDocumentUrn();
    const [expandedGroupKeys, setExpandedGroupKeys] = useState<Set<string>>(new Set());

    const { data: selectedData } = useGetDocumentQuery({
        skip: !selectedUrn,
        // Parent chain needed when the open doc is prepended outside the loaded page.
        variables: { urn: selectedUrn ?? '', includeParentDocuments: true },
        fetchPolicy: 'cache-first',
    });
    // cache-first can still hold the previous URL's document while the next one loads.
    const loadedDocument = (selectedData?.document ?? null) as Document | null;
    const selectedDocument = loadedDocument?.urn === selectedUrn ? loadedDocument : null;

    const activeGroup = useMemo(
        () =>
            resolveActiveDocumentDomainGroup(
                toDocumentGroupingEntityData(selectedDocument),
                selectedUrn,
                t('context.groupBy.unassigned'),
                (entity) => entityRegistry.getDisplayName(entity.type, entity),
            ),
        [entityRegistry, selectedDocument, selectedUrn, t],
    );

    const { groups, loading, refetch } = useDocumentGroupRoots(t('context.groupBy.unassigned'), activeGroup, viewUrn);
    const previousActiveGroupRef = useRef<{ urn: string | null; groupKey: string | null } | undefined>(undefined);

    useEffect(() => {
        const next = { urn: selectedDocument?.urn ?? null, groupKey: activeGroup?.key ?? null };
        const previous = previousActiveGroupRef.current;
        previousActiveGroupRef.current = next;
        if (!shouldRefetchDocumentDomainGroups(previous?.urn, previous?.groupKey, next.urn, next.groupKey)) {
            return;
        }
        refetch();
        // Header refetch is not enough: expanded group pages stay cached by group
        // key. Remount only when this same document changed domains, so selecting
        // another document cannot put the moved doc back in the group it left.
        onDocumentMovedBetweenGroups?.();
    }, [activeGroup, onDocumentMovedBetweenGroups, refetch, selectedDocument]);

    /**
     * Groups start collapsed: each expanded group runs its own paginated search,
     * so expanding every group up front would fire one request per domain at once.
     * Only the open document's group is opened below.
     */
    useEffect(() => {
        if (!activeGroup) return;
        setExpandedGroupKeys((current) => {
            if (current.has(activeGroup.key)) return current;
            return new Set(current).add(activeGroup.key);
        });
    }, [activeGroup]);

    if (loading && groups.length === 0) {
        return <Loading height={16} marginTop={0} />;
    }

    if (!loading && groups.length === 0) {
        return (
            <EmptyStateWrapper>
                <EmptyState
                    icon={FileText}
                    title={t('document.noDocumentsTitle')}
                    description={t('document.noDocumentsSubtitle')}
                    size="sm"
                />
            </EmptyStateWrapper>
        );
    }

    return (
        <div data-testid="document-sidebar-domain-groups">
            {groups.map((group) => (
                <DocumentGroupSection
                    key={group.key}
                    group={group}
                    sort={sort}
                    isExpanded={expandedGroupKeys.has(group.key)}
                    selectedUrn={activeGroup?.key === group.key ? selectedUrn : null}
                    selectedDocument={selectedDocument}
                    suppressSelectionScroll={suppressSelectionScroll}
                    onToggle={() =>
                        setExpandedGroupKeys((current) => {
                            const next = new Set(current);
                            if (next.has(group.key)) next.delete(group.key);
                            else next.add(group.key);
                            return next;
                        })
                    }
                    onCreateChild={onCreateChild}
                    onSelect={onSelect}
                />
            ))}
        </div>
    );
}
