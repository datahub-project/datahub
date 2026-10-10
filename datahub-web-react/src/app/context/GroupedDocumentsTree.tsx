import { EmptyState } from '@components';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import DocumentGroupSection from '@app/context/DocumentGroupSection';
import useDocumentGroupRoots from '@app/document/hooks/useDocumentGroupRoots';
import { useDocumentNavigation } from '@app/document/hooks/useDocumentNavigation';
import {
    resolveActiveDocumentDomainGroup,
    toDocumentGroupingEntityData,
} from '@app/document/utils/documentSidebarGrouping';
import { DocumentSidebarSortValue } from '@app/document/utils/documentSidebarSort';
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
    onCreateChild?: (parentUrn: string) => void;
    onSelect: (urn: string) => void;
};

export default function GroupedDocumentsTree({ sort, viewUrn, onCreateChild, onSelect }: Props) {
    const { t } = useTranslation('misc');
    const entityRegistry = useEntityRegistry();
    const { getCurrentDocumentUrn } = useDocumentNavigation();
    const selectedUrn = getCurrentDocumentUrn();
    const [expandedGroupKeys, setExpandedGroupKeys] = useState<Set<string>>(new Set());

    const { data: selectedData } = useGetDocumentQuery({
        skip: !selectedUrn,
        variables: { urn: selectedUrn ?? '', includeParentDocuments: false },
        fetchPolicy: 'cache-first',
    });
    const selectedDocument = (selectedData?.document ?? null) as Document | null;

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

    const { groups, loading } = useDocumentGroupRoots(t('context.groupBy.unassigned'), activeGroup, viewUrn);

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
