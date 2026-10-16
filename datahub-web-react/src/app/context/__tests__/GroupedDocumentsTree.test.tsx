import { render } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import GroupedDocumentsTree from '@app/context/GroupedDocumentsTree';
import { DEFAULT_DOCUMENT_SIDEBAR_SORT } from '@app/document/utils/documentSidebarSort';

import { EntityType } from '@types';

const mockRefetch = vi.fn();
let selectedDocument: { urn: string; domain: unknown } | null = null;

vi.mock('@app/document/hooks/useDocumentNavigation', () => ({
    useDocumentNavigation: () => ({
        getCurrentDocumentUrn: () => 'urn:li:document:moved',
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
    default: () => null,
}));

describe('GroupedDocumentsTree', () => {
    beforeEach(() => {
        mockRefetch.mockReset();
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
});
