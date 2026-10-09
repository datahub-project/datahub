import { FolderSimple } from '@phosphor-icons/react/dist/csr/FolderSimple';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import useDocumentGroupDocuments from '@app/document/hooks/useDocumentGroupDocuments';
import {
    DocumentDomainGroup,
    buildDocumentParentBreadcrumb,
    getDocumentSidebarTitle,
} from '@app/document/utils/documentSidebarGrouping';
import { DocumentSidebarSortValue } from '@app/document/utils/documentSidebarSort';
import { isDocumentUnpublished, isExternalDocument } from '@app/document/utils/documentUtils';
import { DomainColoredIcon } from '@app/entityV2/shared/links/DomainColoredIcon';
import { DocumentTreeItem } from '@app/homeV2/layout/sidebar/documents/DocumentTreeItem';
import { TreeSectionHeader } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeSectionHeader';

import { Document } from '@types';

const GroupIcon = styled.span`
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
`;

type Props = {
    group: DocumentDomainGroup;
    sort: DocumentSidebarSortValue;
    isExpanded: boolean;
    selectedUrn: string | null;
    selectedDocument?: Document | null;
    onToggle: () => void;
    onCreateChild?: (parentUrn: string) => void;
    onSelect: (urn: string) => void;
};

export default function DocumentGroupSection({
    group,
    sort,
    isExpanded,
    selectedUrn,
    selectedDocument,
    onToggle,
    onCreateChild,
    onSelect,
}: Props) {
    const { t: tet } = useTranslation('entity.types');
    const { t: th } = useTranslation('home.v2');
    const theme = useTheme();
    const { documents, scrollRef } = useDocumentGroupDocuments({
        groupKey: group.key,
        sort,
        skip: !isExpanded,
        selectedDocument: selectedUrn && selectedDocument?.urn === selectedUrn ? selectedDocument : null,
    });

    const icon = group.entity ? (
        <DomainColoredIcon domain={group.entity} size={20} fontSize={12} />
    ) : (
        <FolderSimple color={theme.colors.icon} size={16} />
    );

    return (
        <>
            <TreeSectionHeader
                level={0}
                label={group.label}
                icon={<GroupIcon>{icon}</GroupIcon>}
                isExpanded={isExpanded}
                onToggle={onToggle}
                testId={`document-sidebar-group-${group.key}`}
            />
            {isExpanded &&
                documents.map((doc) => {
                    const title = getDocumentSidebarTitle(doc, tet('document.untitledFallback'));
                    const breadcrumb = buildDocumentParentBreadcrumb(doc, th('untitled'));
                    return (
                        <DocumentTreeItem
                            key={doc.urn}
                            urn={doc.urn}
                            title={title}
                            level={1}
                            hasChildren={false}
                            isExpanded={false}
                            isSelected={selectedUrn === doc.urn}
                            isUnpublished={isDocumentUnpublished(doc)}
                            isExternal={isExternalDocument(doc)}
                            platform={doc.platform}
                            belowLabel={breadcrumb}
                            onToggleExpand={() => {}}
                            onClick={() => onSelect(doc.urn)}
                            onCreateChild={onCreateChild ?? (() => {})}
                            hideCreate={!onCreateChild}
                            hideActionsMenu
                            parentUrn={doc.parentDocuments?.documents?.[0]?.urn ?? null}
                        />
                    );
                })}
            {isExpanded && <div ref={scrollRef} style={{ height: 1 }} />}
        </>
    );
}
