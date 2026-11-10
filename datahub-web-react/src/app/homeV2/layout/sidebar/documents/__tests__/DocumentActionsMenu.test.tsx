import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { DocumentActionsMenu } from '@app/homeV2/layout/sidebar/documents/DocumentActionsMenu';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

vi.mock('@app/document/hooks/useDocumentTreeMutations', () => ({
    useDeleteDocumentTreeMutation: () => ({
        deleteDocument: vi.fn().mockResolvedValue(true),
    }),
}));

vi.mock('@app/document/DocumentTreeContext', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@app/document/DocumentTreeContext')>();
    return {
        ...actual,
        useDocumentTree: () => ({
            getNode: () => undefined,
            getRootNodes: () => [],
        }),
    };
});

const NESTED_DELETE_TEXT =
    'Are you sure you want to delete this document? This will also delete every nested document under it.';

describe('DocumentActionsMenu', () => {
    it('confirms that deleting a document also deletes every nested document', () => {
        render(
            <MockedProvider>
                <TestPageContainer>
                    <DocumentActionsMenu documentUrn="urn:li:document:doc-1" />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.queryByText(NESTED_DELETE_TEXT)).not.toBeInTheDocument();

        fireEvent.click(screen.getByTestId('document-actions-menu-button'));
        fireEvent.click(screen.getByText('Delete'));

        expect(screen.getByText('Delete Document(s)')).toBeInTheDocument();
        expect(screen.getByText(NESTED_DELETE_TEXT)).toBeInTheDocument();
    });
});
