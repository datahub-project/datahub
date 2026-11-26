import { render } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import GroupedDocumentsTree from '@app/context/GroupedDocumentsTree';
import { DEFAULT_DOCUMENT_SIDEBAR_SORT } from '@app/document/utils/documentSidebarSort';

import { EntityType } from '@types';

const mockRefetch = vi.fn();
let currentUrn = 'urn:li:document:moved';
let selectedDocument: { urn: string; domain: unknown } | null = null;
const renderedSections: Array<{ selectedDocument: { urn?: string } | null }> = [];

vi.mock('@app/document/hooks/useDocumentNavigation', () => ({
    useDocumentNavigation: () => ({
        getCurrentDocumentUrn: () => currentUrn,
    }),
}));

vi.mock('@graphql/document.generated', () => ({
    useGetDocumentQuery: () => ({
        data: selectedDocument ? { document: selectedDocument } : undefined,
        loading: false,
    }),
}));

vi.mock('@app/document/hooks/useDocumentGroupRoots', () => ({
    default: () => ({
        groups: [{ key: 'urn:li:domain:marketing', label: 'Marketing' }],
        loading: false,
        refetch: mockRefetch,
    }),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: () => 'Marketing',
    }),
}));

vi.mock('@app/context/DocumentGroupSection', () => ({
    default: (props: { selectedDocument: { urn?: string } | null }) => {
        renderedSections.push({ selectedDocument: props.selectedDocument });
        return null;
    },
}));

describe('GroupedDocumentsTree', () => {
    beforeEach(() => {
        mockRefetch.mockReset();
        renderedSections.length = 0;
        currentUrn = 'urn:li:document:moved';
        selectedDocument = {
            urn: 'urn:li:document:moved',
            domain: null,
        };
    });

    it('asks the sidebar to remount group pages when the open document changes domains', () => {
        const onDocumentMovedBetweenGroups = vi.fn();
        const { rerender } = render(
            <GroupedDocumentsTree
                sort={DEFAULT_DOCUMENT_SIDEBAR_SORT}
                onSelect={vi.fn()}
                onDocumentMovedBetweenGroups={onDocumentMovedBetweenGroups}
            />,
        );

        expect(onDocumentMovedBetweenGroups).not.toHaveBeenCalled();

        selectedDocument = {
            urn: 'urn:li:document:moved',
            domain: {
                domain: { urn: 'urn:li:domain:marketing', type: EntityType.Domain },
            },
        };
        rerender(
            <GroupedDocumentsTree
                sort={DEFAULT_DOCUMENT_SIDEBAR_SORT}
                onSelect={vi.fn()}
                onDocumentMovedBetweenGroups={onDocumentMovedBetweenGroups}
            />,
        );

        expect(onDocumentMovedBetweenGroups).toHaveBeenCalledTimes(1);
        expect(mockRefetch).toHaveBeenCalled();
    });

    it('does not remount group pages when opening a different document in another domain', () => {
        const onDocumentMovedBetweenGroups = vi.fn();
        const { rerender } = render(
            <GroupedDocumentsTree
                sort={DEFAULT_DOCUMENT_SIDEBAR_SORT}
                onSelect={vi.fn()}
                onDocumentMovedBetweenGroups={onDocumentMovedBetweenGroups}
            />,
        );

        currentUrn = 'urn:li:document:other';
        selectedDocument = {
            urn: 'urn:li:document:other',
            domain: {
                domain: { urn: 'urn:li:domain:marketing', type: EntityType.Domain },
            },
        };
        rerender(
            <GroupedDocumentsTree
                sort={DEFAULT_DOCUMENT_SIDEBAR_SORT}
                onSelect={vi.fn()}
                onDocumentMovedBetweenGroups={onDocumentMovedBetweenGroups}
            />,
        );

        expect(onDocumentMovedBetweenGroups).not.toHaveBeenCalled();
        expect(mockRefetch).not.toHaveBeenCalled();
    });

    it('does not apply a cached document from the previous URL', () => {
        currentUrn = 'urn:li:document:next';
        selectedDocument = {
            urn: 'urn:li:document:moved',
            domain: {
                domain: { urn: 'urn:li:domain:marketing', type: EntityType.Domain },
            },
        };

        render(
            <GroupedDocumentsTree
                sort={DEFAULT_DOCUMENT_SIDEBAR_SORT}
                onSelect={vi.fn()}
                onDocumentMovedBetweenGroups={vi.fn()}
            />,
        );

        expect(renderedSections.every((section) => section.selectedDocument == null)).toBe(true);
    });
});
