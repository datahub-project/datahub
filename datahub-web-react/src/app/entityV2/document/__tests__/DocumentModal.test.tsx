import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DocumentModal } from '@app/entityV2/document/DocumentModal';
import { EditableContent } from '@app/entityV2/document/summary/EditableContent';
import { AppConfigContext, DEFAULT_APP_CONFIG } from '@src/appConfigContext';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const DOCUMENT_URN = 'urn:li:document:1';

vi.mock('@graphql/document.generated', () => ({
    useGetDocumentQuery: () => ({
        data: { document: { urn: DOCUMENT_URN, info: { contents: { text: 'hello' } } } },
        loading: false,
        refetch: vi.fn().mockResolvedValue(undefined),
    }),
}));

vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    PageTemplateProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

// Render only the body editor so the test exercises the modal <-> editor unsaved-changes handshake.
vi.mock('@app/entityV2/document/summary/DocumentSummaryTab', () => ({
    DocumentSummaryTab: () => <EditableContent documentUrn={DOCUMENT_URN} initialContent="hello" />,
}));

vi.mock('@components', async () => {
    const actual = await vi.importActual<typeof import('@components')>('@components');
    return {
        ...actual,
        Editor: ({
            content,
            onChange,
            belowToolbar,
            ...rest
        }: {
            content?: string;
            onChange?: (value: string) => void;
            belowToolbar?: React.ReactNode;
            'data-testid'?: string;
        }) => (
            <>
                <textarea
                    data-testid={rest['data-testid']}
                    value={content ?? ''}
                    onChange={(event) => onChange?.(event.target.value)}
                />
                {belowToolbar}
            </>
        ),
    };
});

vi.mock('@app/document/hooks/useDocumentPermissions', () => ({
    useDocumentPermissions: () => ({ canEditContents: true }),
}));

vi.mock('@app/document/hooks/useUpdateDocument', () => ({
    useUpdateDocument: () => ({
        updateContents: vi.fn().mockResolvedValue(true),
        updateRelatedEntities: vi.fn().mockResolvedValue(true),
    }),
}));

vi.mock('@app/shared/hooks/useFileUpload', () => ({
    default: () => ({ uploadFile: vi.fn() }),
}));

function renderModal(onClose: () => void) {
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <AppConfigContext.Provider
                    value={{
                        loaded: true,
                        refreshContext: () => undefined,
                        config: {
                            ...DEFAULT_APP_CONFIG,
                            featureFlags: { ...DEFAULT_APP_CONFIG.featureFlags, documentExplicitSaveEnabled: true },
                        },
                    }}
                >
                    <DocumentModal documentUrn={DOCUMENT_URN} onClose={onClose} />
                </AppConfigContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('DocumentModal with explicit save', () => {
    const onClose = vi.fn();

    beforeEach(() => {
        onClose.mockClear();
    });

    it('closes immediately when there are no unsaved edits', () => {
        renderModal(onClose);

        fireEvent.click(screen.getByTestId('modal-close-icon'));

        expect(onClose).toHaveBeenCalledTimes(1);
    });

    it('asks before discarding unsaved edits on close', () => {
        renderModal(onClose);

        fireEvent.change(screen.getByTestId('document-content-editor'), { target: { value: 'hello world' } });
        fireEvent.click(screen.getByTestId('modal-close-icon'));

        expect(onClose).not.toHaveBeenCalled();
        expect(screen.getByTestId('modal-confirm-button')).toBeInTheDocument();

        // Secondary action discards the edits and lets the modal close.
        fireEvent.click(screen.getByTestId('modal-cancel-button'));

        expect(onClose).toHaveBeenCalledTimes(1);
    });
});
